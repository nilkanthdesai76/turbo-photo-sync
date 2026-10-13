import Foundation
import Photos
import SwiftUI
import UIKit

struct BatchSummary: Identifiable, Codable {
    let id: String
    let index: Int
    let totalFiles: Int
    let photosCount: Int
    let videosCount: Int
    let totalBytes: Int64
    var syncedFiles: Int
    let albums: [String]
}

struct BatchesResponse: Codable {
    let batches: [BatchSummary]
}

struct ServerStatusResponse: Codable {
    let status: String
    let serverIP: String
    let usbTunnelIP: String?
    let usbURL: String?
    let wifiURL: String?
    let port: Int
    let totalBatches: Int
    let totalFiles: Int
    let totalBytes: Int64
    let syncedFiles: Int
}

struct RemoteMediaFile: Identifiable, Codable, Sendable {
    var id: String { key }
    let key: String
    let filename: String
    let size: Int64
    let mtime: Double
    let isVideo: Bool
    let album: String?
    let url: String
    var synced: Bool
}

struct BatchDetailResponse: Codable {
    let id: String
    let index: Int
    let totalFiles: Int
    let totalBytes: Int64
    let albums: [String]
    let files: [RemoteMediaFile]
}

struct StagedAsset: @unchecked Sendable {
    let item: RemoteMediaFile
    let tempURL: URL
    let cleanOriginalName: String
    let albumCollection: PHAssetCollection?
}

enum TransportMode: String, CaseIterable, Identifiable {
    case dual = "🚀 Dual (USB+5G)"
    case usb = "⚡ USB Only"
    case wifi = "📶 5G Wi-Fi"
    var id: String { rawValue }
}

/// Actor that guarantees serialized, thread-safe PhotoKit transactions across all parallel batches.
actor PhotoKitCommitter {
    static let shared = PhotoKitCommitter()

    func commitChunk(chunk: [StagedAsset]) async {
        guard !chunk.isEmpty else { return }
        do {
            try await PHPhotoLibrary.shared().performChanges {
                for asset in chunk {
                    let creationRequest = PHAssetCreationRequest.forAsset()
                    if asset.item.mtime > 0 {
                        creationRequest.creationDate = Date(timeIntervalSince1970: asset.item.mtime)
                    }
                    let options = PHAssetResourceCreationOptions()
                    options.originalFilename = asset.cleanOriginalName
                    options.shouldMoveFile = true

                    let resType: PHAssetResourceType = asset.item.isVideo ? .video : .photo
                    creationRequest.addResource(with: resType, fileURL: asset.tempURL, options: options)

                    if let album = asset.albumCollection,
                       let placeholder = creationRequest.placeholderForCreatedAsset,
                       let albumChangeReq = PHAssetCollectionChangeRequest(for: album) {
                        albumChangeReq.addAssets([placeholder] as NSArray)
                    }
                }
            }

            for asset in chunk {
                try? FileManager.default.removeItem(at: asset.tempURL)
            }
        } catch {
            // Fallback: If any single file causes batch error, commit individually
            for asset in chunk {
                do {
                    try await PHPhotoLibrary.shared().performChanges {
                        let req = PHAssetCreationRequest.forAsset()
                        if asset.item.mtime > 0 {
                            req.creationDate = Date(timeIntervalSince1970: asset.item.mtime)
                        }
                        let opts = PHAssetResourceCreationOptions()
                        opts.originalFilename = asset.cleanOriginalName
                        opts.shouldMoveFile = true
                        let resType: PHAssetResourceType = asset.item.isVideo ? .video : .photo
                        req.addResource(with: resType, fileURL: asset.tempURL, options: opts)
                        if let album = asset.albumCollection,
                           let placeholder = req.placeholderForCreatedAsset,
                           let albumChangeReq = PHAssetCollectionChangeRequest(for: album) {
                            albumChangeReq.addAssets([placeholder] as NSArray)
                        }
                    }
                } catch { }
                try? FileManager.default.removeItem(at: asset.tempURL)
            }
        }
    }
}

@MainActor
final class SyncManager: ObservableObject {
    @AppStorage("serverBaseURL") var serverBaseURL: String = "http://localhost:8765"
    @AppStorage("usbServerURL") var usbServerURL: String = "http://localhost:8765"
    @AppStorage("wifiServerURL") var wifiServerURL: String = "http://192.168.1.100:8765"
    @AppStorage("createAlbumsOnPhone") var createAlbumsOnPhone: Bool = true
    @AppStorage("parallelWorkerCount") var parallelWorkerCount: Int = 16

    @Published var transportMode: TransportMode = .usb
    @Published var isConnected: Bool = false
    @Published var usbReachable: Bool = false
    @Published var wifiReachable: Bool = false
    @Published var statusMessage: String = "Probing USB Cable + 5GHz Wi-Fi..."
    @Published var batches: [BatchSummary] = []
    @Published var totalServerFiles: Int = 0
    @Published var totalSyncedFiles: Int = 0

    // Multi-batch state tracking
    @Published var isSyncing: Bool = false
    @Published var activeBatchIds: Set<String> = []
    @Published var batchLiveDone: [String: Int] = [:]
    @Published var batchLiveTotal: [String: Int] = [:]
    @Published var isAllBatchesQueueActive: Bool = false

    // Compatibility computed properties
    var activeBatchId: String? { activeBatchIds.first }
    var batchProgressDone: Int { activeBatchIds.first.flatMap { batchLiveDone[$0] } ?? 0 }
    var batchProgressTotal: Int { activeBatchIds.first.flatMap { batchLiveTotal[$0] } ?? 0 }

    @Published var currentFileName: String = ""
    @Published var speedMBps: Double = 0.0
    @Published var errorBanner: String? = nil
    @Published var photoAuthStatus: PHAuthorizationStatus = .notDetermined

    private var batchTasks: [String: Task<Void, Never>] = [:]
    private var batchCancelFlags: [String: Bool] = [:]
    private var allBatchesTask: Task<Void, Never>? = nil

    private var albumObjectCache: [String: PHAssetCollection] = [:]
    private let urlSession: URLSession

    // Speed telemetry
    private var rollingBytesTransferred: Int64 = 0
    private var rollingStartTime: Date = Date()

    init() {
        let config = URLSessionConfiguration.default
        config.httpMaximumConnectionsPerHost = 64
        config.timeoutIntervalForRequest = 60
        config.timeoutIntervalForResource = 600
        config.requestCachePolicy = .reloadIgnoringLocalCacheData
        self.urlSession = URLSession(configuration: config)
        self.photoAuthStatus = PHPhotoLibrary.authorizationStatus(for: .readWrite)
    }

    func switchTransport(_ mode: TransportMode) {
        transportMode = mode
        switch mode {
        case .usb:
            serverBaseURL = usbServerURL
        case .wifi:
            serverBaseURL = wifiServerURL
        case .dual:
            serverBaseURL = usbReachable ? usbServerURL : wifiServerURL
        }
        Task {
            await refreshDashboard()
        }
    }

    func requestPhotoPermission() async -> Bool {
        let status = await PHPhotoLibrary.requestAuthorization(for: .readWrite)
        self.photoAuthStatus = status
        return status == .authorized || status == .limited
    }

    private func probeEndpoint(_ base: String) async -> ServerStatusResponse? {
        let clean = base.trimmingCharacters(in: .whitespacesAndNewlines).replacingOccurrences(of: "/+$", with: "", options: .regularExpression)
        guard let url = URL(string: "\(clean)/api/status") else { return nil }
        var req = URLRequest(url: url)
        req.timeoutInterval = 2.5
        do {
            let (data, resp) = try await urlSession.data(for: req)
            guard (resp as? HTTPURLResponse)?.statusCode == 200 else { return nil }
            return try JSONDecoder().decode(ServerStatusResponse.self, from: data)
        } catch {
            return nil
        }
    }

    func refreshDashboard() async {
        async let wifiProbe = probeEndpoint(wifiServerURL)
        async let usbProbe = probeEndpoint(usbServerURL)

        let (wStat, uStat) = await (wifiProbe, usbProbe)

        if let s = wStat ?? uStat {
            if let discoveredWiFi = s.wifiURL { self.wifiServerURL = discoveredWiFi }
            if let discoveredUSB = s.usbURL { self.usbServerURL = discoveredUSB }
        }

        var finalUSBStat = uStat
        if finalUSBStat == nil, let discoveredUSB = wStat?.usbURL {
            finalUSBStat = await probeEndpoint(discoveredUSB)
        }

        self.wifiReachable = (wStat != nil)
        self.usbReachable = (finalUSBStat != nil)

        guard let activeStat = finalUSBStat ?? wStat else {
            self.isConnected = false
            self.statusMessage = "Offline • Make sure Mac Server is running"
            return
        }

        let controlBase: String
        switch transportMode {
        case .usb:
            controlBase = usbReachable ? usbServerURL : wifiServerURL
        case .wifi:
            controlBase = wifiReachable ? wifiServerURL : usbServerURL
        case .dual:
            controlBase = usbReachable ? usbServerURL : wifiServerURL
        }
        self.serverBaseURL = controlBase

        guard let batchesURL = URL(string: "\(controlBase)/api/batches") else { return }
        do {
            let (bData, _) = try await urlSession.data(from: batchesURL)
            let bResp = try JSONDecoder().decode(BatchesResponse.self, from: bData)

            self.isConnected = true
            self.totalServerFiles = activeStat.totalFiles
            self.totalSyncedFiles = activeStat.syncedFiles
            self.batches = bResp.batches
            self.errorBanner = nil

            for b in bResp.batches {
                if !self.activeBatchIds.contains(b.id) {
                    self.batchLiveDone[b.id] = b.syncedFiles
                    self.batchLiveTotal[b.id] = b.totalFiles
                }
            }

            if usbReachable && wifiReachable && transportMode == .dual {
                self.statusMessage = "🚀 Dual Turbo Active: USB Cable + 5GHz Wi-Fi (\(parallelWorkerCount) Nodes/Batch)"
            } else if usbReachable && (transportMode == .usb || transportMode == .dual) {
                self.statusMessage = "⚡ Connected via USB Cable Tunnel (MTU 16000, \(parallelWorkerCount) Nodes/Batch)"
            } else {
                self.statusMessage = "📶 Connected via 5GHz Wi-Fi (\(activeStat.serverIP):\(activeStat.port))"
            }
        } catch {
            self.isConnected = false
            self.statusMessage = "Error loading batches: \(error.localizedDescription)"
        }
    }

    func stopSync() {
        isAllBatchesQueueActive = false
        allBatchesTask?.cancel()
        allBatchesTask = nil

        for (bId, task) in batchTasks {
            batchCancelFlags[bId] = true
            task.cancel()
        }
        batchTasks.removeAll()
        activeBatchIds.removeAll()
        isSyncing = false
        UIApplication.shared.isIdleTimerDisabled = false
        statusMessage = "All sync operations stopped."
    }

    func stopBatch(_ batchId: String) {
        batchCancelFlags[batchId] = true
        if let task = batchTasks[batchId] {
            task.cancel()
            batchTasks.removeValue(forKey: batchId)
        }
        activeBatchIds.remove(batchId)
        isSyncing = !activeBatchIds.isEmpty
        if activeBatchIds.isEmpty {
            UIApplication.shared.isIdleTimerDisabled = false
        }
    }

    private func isBatchCancelled(_ batchId: String) -> Bool {
        return batchCancelFlags[batchId] == true || Task.isCancelled
    }

    func syncBatch(_ batchId: String) {
        guard !activeBatchIds.contains(batchId) else { return }
        activeBatchIds.insert(batchId)
        batchCancelFlags[batchId] = false
        isSyncing = true
        UIApplication.shared.isIdleTimerDisabled = true

        let task = Task { @MainActor [weak self] in
            guard let self = self else { return }
            guard await self.requestPhotoPermission() else {
                self.errorBanner = "Please grant Full Photo Library Access in Settings."
                self.activeBatchIds.remove(batchId)
                self.isSyncing = !self.activeBatchIds.isEmpty
                return
            }

            await self.syncSingleBatchInternal(batchId: batchId)
            self.activeBatchIds.remove(batchId)
            self.batchTasks.removeValue(forKey: batchId)
            self.isSyncing = !self.activeBatchIds.isEmpty
            if self.activeBatchIds.isEmpty {
                UIApplication.shared.isIdleTimerDisabled = false
            }
            await self.refreshDashboard()
        }
        batchTasks[batchId] = task
    }

    func syncAllRemainingBatches(concurrentBatches: Int = 2) {
        guard !isAllBatchesQueueActive else { return }
        isAllBatchesQueueActive = true

        allBatchesTask = Task { @MainActor [weak self] in
            guard let self = self else { return }
            guard await self.requestPhotoPermission() else {
                self.errorBanner = "Please grant Full Photo Library Access in Settings."
                self.isAllBatchesQueueActive = false
                return
            }
            await self.refreshDashboard()

            let unsynced = self.batches.filter { $0.syncedFiles < $0.totalFiles }
            var batchIndex = 0

            while batchIndex < unsynced.count && !Task.isCancelled && self.isAllBatchesQueueActive {
                while self.activeBatchIds.count < concurrentBatches && batchIndex < unsynced.count {
                    let candidate = unsynced[batchIndex]
                    batchIndex += 1
                    if !self.activeBatchIds.contains(candidate.id) && (self.batchLiveDone[candidate.id] ?? candidate.syncedFiles) < candidate.totalFiles {
                        self.syncBatch(candidate.id)
                    }
                }

                try? await Task.sleep(nanoseconds: 500_000_000)

                if self.activeBatchIds.isEmpty && batchIndex >= unsynced.count {
                    break
                }
            }

            while !self.activeBatchIds.isEmpty && !Task.isCancelled && self.isAllBatchesQueueActive {
                try? await Task.sleep(nanoseconds: 500_000_000)
            }

            self.isAllBatchesQueueActive = false
            self.allBatchesTask = nil
            await self.refreshDashboard()
            if self.activeBatchIds.isEmpty {
                self.statusMessage = "All batches saved to iPhone Photos!"
            }
        }
    }

    private func activeEndpoints() -> [String] {
        switch transportMode {
        case .dual:
            var list: [String] = []
            if usbReachable { list.append(usbServerURL) }
            if wifiReachable { list.append(wifiServerURL) }
            return list.isEmpty ? [serverBaseURL] : list
        case .usb:
            return [usbReachable ? usbServerURL : serverBaseURL]
        case .wifi:
            return [wifiReachable ? wifiServerURL : serverBaseURL]
        }
    }

    private func syncSingleBatchInternal(batchId: String) async {
        let endpoints = activeEndpoints()
        let primaryBase = endpoints[0]
        guard let detailURL = URL(string: "\(primaryBase)/api/batches/\(batchId)") else { return }

        do {
            let (data, _) = try await urlSession.data(from: detailURL)
            let detail = try JSONDecoder().decode(BatchDetailResponse.self, from: data)
            let remaining = detail.files.filter { !$0.synced }

            batchLiveTotal[batchId] = detail.totalFiles
            batchLiveDone[batchId] = detail.totalFiles - remaining.count

            if remaining.isEmpty {
                if let bIdx = batches.firstIndex(where: { $0.id == batchId }) {
                    batches[bIdx].syncedFiles = detail.totalFiles
                }
                return
            }

            var cachedAlbums: [String: PHAssetCollection] = [:]
            if createAlbumsOnPhone {
                for albumName in detail.albums {
                    if let col = try? await ensureAlbumCollection(named: albumName) {
                        cachedAlbums[albumName] = col
                    }
                }
            }

            let totalRemainingCount = remaining.count
            let concurrency = max(4, min(32, parallelWorkerCount))
            let commitBatchSize = 25

            let (stream, continuation) = AsyncStream<StagedAsset>.makeStream()

            let consumerTask = Task { @MainActor [weak self] () -> Void in
                guard let self = self else { return }
                var buffer: [StagedAsset] = []
                var pendingSyncedKeys: [String] = []

                for await staged in stream {
                    buffer.append(staged)
                    if buffer.count >= commitBatchSize {
                        let toCommit = buffer
                        buffer.removeAll()
                        await PhotoKitCommitter.shared.commitChunk(chunk: toCommit)
                        self.recordCommittedChunk(chunk: toCommit, batchId: batchId)

                        pendingSyncedKeys.append(contentsOf: toCommit.map { $0.item.key })
                        if pendingSyncedKeys.count >= 25 {
                            let keysToMark = pendingSyncedKeys
                            pendingSyncedKeys.removeAll()
                            Task {
                                await self.markKeysSyncedOnServer(keys: keysToMark, baseURL: primaryBase)
                            }
                        }
                    }
                }

                if !buffer.isEmpty {
                    let toCommit = buffer
                    buffer.removeAll()
                    await PhotoKitCommitter.shared.commitChunk(chunk: toCommit)
                    self.recordCommittedChunk(chunk: toCommit, batchId: batchId)
                    pendingSyncedKeys.append(contentsOf: toCommit.map { $0.item.key })
                }

                if !pendingSyncedKeys.isEmpty {
                    let keysToMark = pendingSyncedKeys
                    pendingSyncedKeys.removeAll()
                    await self.markKeysSyncedOnServer(keys: keysToMark, baseURL: primaryBase)
                }
            }

            var nextIndex = 0
            await withTaskGroup(of: Void.self) { group in
                while nextIndex < min(concurrency, totalRemainingCount) && !self.isBatchCancelled(batchId) {
                    let fileItem = remaining[nextIndex]
                    let chosenBase = endpoints[nextIndex % endpoints.count]
                    let albumCol = fileItem.album.flatMap { cachedAlbums[$0] }
                    nextIndex += 1

                    group.addTask { [weak self] in
                        guard let self = self else { return }
                        if let staged = await self.downloadToStaging(item: fileItem, baseURL: chosenBase, albumCollection: albumCol) {
                            continuation.yield(staged)
                        }
                    }
                }

                while await group.next() != nil {
                    if nextIndex < totalRemainingCount && !self.isBatchCancelled(batchId) {
                        let nextItem = remaining[nextIndex]
                        let chosenBase = endpoints[nextIndex % endpoints.count]
                        let albumCol = nextItem.album.flatMap { cachedAlbums[$0] }
                        nextIndex += 1

                        group.addTask { [weak self] in
                            guard let self = self else { return }
                            if let staged = await self.downloadToStaging(item: nextItem, baseURL: chosenBase, albumCollection: albumCol) {
                                continuation.yield(staged)
                            }
                        }
                    }
                }
            }

            continuation.finish()
            await consumerTask.value
        } catch {
            self.errorBanner = "Batch \(batchId) error: \(error.localizedDescription)"
        }
    }

    private func recordCommittedChunk(chunk: [StagedAsset], batchId: String) {
        let count = chunk.count
        let currentDone = (batchLiveDone[batchId] ?? 0) + count
        batchLiveDone[batchId] = currentDone
        totalSyncedFiles += count

        if let bIdx = batches.firstIndex(where: { $0.id == batchId }) {
            batches[bIdx].syncedFiles = currentDone
        }

        let chunkBytes = chunk.reduce(Int64(0)) { $0 + $1.item.size }
        rollingBytesTransferred += chunkBytes
        if let last = chunk.last {
            currentFileName = last.item.filename
        }

        let elapsed = max(0.2, Date().timeIntervalSince(rollingStartTime))
        speedMBps = (Double(rollingBytesTransferred) / (1024.0 * 1024.0)) / elapsed
        if elapsed > 2.5 {
            rollingBytesTransferred = 0
            rollingStartTime = Date()
        }
    }

    private nonisolated func downloadToStaging(
        item: RemoteMediaFile,
        baseURL: String,
        albumCollection: PHAssetCollection?
    ) async -> StagedAsset? {
        guard let fileURL = URL(string: "\(baseURL)\(item.url)") else { return nil }

        do {
            let (tempDownloadedURL, response) = try await urlSession.download(from: fileURL)
            if let httpResp = response as? HTTPURLResponse, httpResp.statusCode != 200 {
                return nil
            }

            let ext = (item.filename as NSString).pathExtension
            let safeTempURL = FileManager.default.temporaryDirectory
                .appendingPathComponent("stage_\(UUID().uuidString)")
                .appendingPathExtension(ext.isEmpty ? "jpg" : ext)

            try? FileManager.default.removeItem(at: safeTempURL)
            try FileManager.default.moveItem(at: tempDownloadedURL, to: safeTempURL)

            let cleanOriginalName = item.filename.replacingOccurrences(
                of: "__\\d+(\\.[^.]+)$",
                with: "$1",
                options: .regularExpression
            )

            return StagedAsset(
                item: item,
                tempURL: safeTempURL,
                cleanOriginalName: cleanOriginalName,
                albumCollection: albumCollection
            )
        } catch {
            return nil
        }
    }

    private func ensureAlbumCollection(named title: String) async throws -> PHAssetCollection {
        if let cached = albumObjectCache[title] {
            return cached
        }

        let fetchOptions = PHFetchOptions()
        fetchOptions.predicate = NSPredicate(format: "title = %@", title)
        let existing = PHAssetCollection.fetchAssetCollections(with: .album, subtype: .any, options: fetchOptions)
        if let first = existing.firstObject {
            albumObjectCache[title] = first
            return first
        }

        var createdId: String? = nil
        try await PHPhotoLibrary.shared().performChanges {
            let req = PHAssetCollectionChangeRequest.creationRequestForAssetCollection(withTitle: title)
            createdId = req.placeholderForCreatedAssetCollection.localIdentifier
        }
        if let id = createdId,
           let col = PHAssetCollection.fetchAssetCollections(withLocalIdentifiers: [id], options: nil).firstObject {
            albumObjectCache[title] = col
            return col
        }
        throw URLError(.cannotCreateFile)
    }

    private func markKeysSyncedOnServer(keys: [String], baseURL: String) async {
        guard !keys.isEmpty, let url = URL(string: "\(baseURL)/api/mark_synced") else { return }
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.httpBody = try? JSONSerialization.data(withJSONObject: ["keys": keys])
        _ = try? await urlSession.data(for: req)
    }
}
