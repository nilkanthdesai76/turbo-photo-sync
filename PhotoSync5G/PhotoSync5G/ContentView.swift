import SwiftUI

struct ContentView: View {
    @StateObject private var manager = SyncManager()

    var body: some View {
        NavigationStack {
            VStack(spacing: 12) {
                serverConnectionBar
                overallStatusCard

                if let err = manager.errorBanner {
                    Text(err)
                        .font(.footnote.weight(.medium))
                        .foregroundStyle(.white)
                        .padding(10)
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .background(Color.red.opacity(0.88), in: RoundedRectangle(cornerRadius: 10))
                        .padding(.horizontal)
                }

                List(manager.batches) { batch in
                    BatchRowView(
                        batch: batch,
                        isCurrentlySyncingThis: manager.activeBatchIds.contains(batch.id),
                        liveDone: manager.batchLiveDone[batch.id] ?? batch.syncedFiles,
                        onSyncTap: {
                            manager.syncBatch(batch.id)
                        },
                        onStopTap: {
                            manager.stopBatch(batch.id)
                        }
                    )
                }
                .listStyle(.plain)
                .refreshable {
                    await manager.refreshDashboard()
                }
            }
            .navigationTitle("Turbo Photo Sync")
            .navigationBarTitleDisplayMode(.inline)
            .task {
                _ = await manager.requestPhotoPermission()
                await manager.refreshDashboard()
            }
        }
    }

    private var serverConnectionBar: some View {
        VStack(spacing: 8) {
            Picker("Transport", selection: Binding(
                get: { manager.transportMode },
                set: { manager.switchTransport($0) }
            )) {
                ForEach(TransportMode.allCases) { mode in
                    Text(mode.rawValue).tag(mode)
                }
            }
            .pickerStyle(.segmented)
            .disabled(manager.isSyncing)

            HStack {
                Text("Threads / Batch:")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
                Picker("Parallel Nodes", selection: $manager.parallelWorkerCount) {
                    Text("8").tag(8)
                    Text("16 Turbo").tag(16)
                    Text("24 Max").tag(24)
                    Text("32 Extreme").tag(32)
                }
                .pickerStyle(.segmented)
            }

            HStack(spacing: 8) {
                Circle()
                    .fill(manager.isConnected ? Color.green : Color.red)
                    .frame(width: 10, height: 10)

                TextField(
                    manager.transportMode == .wifi ? "http://192.168.1.xxx:8765" : "http://[fd00::2]:8765",
                    text: manager.transportMode == .wifi ? $manager.wifiServerURL : $manager.usbServerURL
                )
                    .font(.system(.subheadline, design: .monospaced))
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
                    .keyboardType(.URL)
                    .padding(.horizontal, 10)
                    .padding(.vertical, 7)
                    .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 8))

                Button {
                    Task { await manager.refreshDashboard() }
                } label: {
                    Image(systemName: "arrow.clockwise")
                        .font(.subheadline.weight(.semibold))
                        .padding(8)
                        .background(Color.accentColor.opacity(0.15), in: RoundedRectangle(cornerRadius: 8))
                }
            }

            HStack(spacing: 12) {
                Label(
                    manager.usbReachable ? "USB Cable Ready" : "USB Unplugged",
                    systemImage: manager.usbReachable ? "cable.connector" : "cable.connector.slash"
                )
                .font(.caption2.weight(.semibold))
                .foregroundStyle(manager.usbReachable ? .green : .secondary)

                Label(
                    manager.wifiReachable ? "5GHz Wi-Fi Ready" : "Wi-Fi Offline",
                    systemImage: manager.wifiReachable ? "wifi" : "wifi.slash"
                )
                .font(.caption2.weight(.semibold))
                .foregroundStyle(manager.wifiReachable ? .green : .secondary)

                Spacer()
            }

            Text(manager.statusMessage)
                .font(.caption)
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding(.horizontal)
        .padding(.top, 6)
    }

    private var overallStatusCard: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                VStack(alignment: .leading, spacing: 2) {
                    Text("Saved to iPhone Photos")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text("\(manager.totalSyncedFiles.formatted()) / \(manager.totalServerFiles.formatted())")
                        .font(.title3.weight(.bold))
                }
                Spacer()
                if manager.isSyncing {
                    VStack(alignment: .trailing, spacing: 2) {
                        Text(String(format: "%.1f MB/s", manager.speedMBps))
                            .font(.subheadline.weight(.bold))
                            .foregroundStyle(.green)
                        Text("\(manager.activeBatchIds.count) \(manager.activeBatchIds.count == 1 ? "Batch" : "Batches") Active")
                            .font(.caption2.weight(.bold))
                            .foregroundStyle(.blue)
                        Text(manager.currentFileName)
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                            .frame(maxWidth: 140, alignment: .trailing)
                    }
                }
            }

            ProgressView(
                value: Double(manager.totalSyncedFiles),
                total: Double(max(1, manager.totalServerFiles))
            )
            .tint(.blue)

            Toggle("Auto-create Albums (Goa, Family, etc.)", isOn: $manager.createAlbumsOnPhone)
                .font(.caption)

            HStack(spacing: 10) {
                if manager.isSyncing {
                    Button(role: .destructive) {
                        manager.stopSync()
                    } label: {
                        Label("Stop All (\(manager.activeBatchIds.count))", systemImage: "stop.fill")
                            .font(.subheadline.weight(.semibold))
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 10)
                    }
                    .buttonStyle(.borderedProminent)
                    .tint(.red)
                }

                Button {
                    manager.syncAllRemainingBatches(concurrentBatches: 2)
                } label: {
                    Label(
                        manager.isAllBatchesQueueActive ? "Queue Running..." : "Sync All Batches",
                        systemImage: "bolt.horizontal.fill"
                    )
                    .font(.subheadline.weight(.semibold))
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 10)
                }
                .buttonStyle(.borderedProminent)
                .disabled(!manager.isConnected || manager.isAllBatchesQueueActive)
            }
        }
        .padding(14)
        .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 14))
        .padding(.horizontal)
    }
}

struct BatchRowView: View {
    let batch: BatchSummary
    let isCurrentlySyncingThis: Bool
    let liveDone: Int
    let onSyncTap: () -> Void
    let onStopTap: () -> Void

    private var isComplete: Bool {
        liveDone >= batch.totalFiles && batch.totalFiles > 0
    }

    private var sizeString: String {
        let gb = Double(batch.totalBytes) / (1024 * 1024 * 1024)
        if gb >= 1.0 {
            return String(format: "%.2f GB", gb)
        }
        let mb = Double(batch.totalBytes) / (1024 * 1024)
        return String(format: "%.0f MB", mb)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                VStack(alignment: .leading, spacing: 3) {
                    HStack(spacing: 6) {
                        Text(batch.id.replacingOccurrences(of: "_", with: " "))
                            .font(.headline)
                        Text("• \(sizeString)")
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                    }

                    Text("\(batch.photosCount) photos, \(batch.videosCount) videos")
                        .font(.caption)
                        .foregroundStyle(.secondary)

                    if !batch.albums.isEmpty {
                        Text("Albums: \(batch.albums.joined(separator: ", "))")
                            .font(.caption2)
                            .foregroundStyle(.blue)
                            .lineLimit(1)
                    }
                }

                Spacer()

                if isComplete {
                    Label("Saved", systemImage: "checkmark.circle.fill")
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(.green)
                } else if isCurrentlySyncingThis {
                    HStack(spacing: 8) {
                        VStack(alignment: .trailing, spacing: 2) {
                            ProgressView()
                                .controlSize(.small)
                            Text("\(liveDone)/\(batch.totalFiles)")
                                .font(.caption.monospacedDigit())
                                .foregroundStyle(.blue)
                        }

                        Button(action: onStopTap) {
                            Image(systemName: "stop.circle.fill")
                                .font(.title3)
                                .foregroundStyle(.red)
                        }
                        .buttonStyle(.plain)
                    }
                } else {
                    Button(action: onSyncTap) {
                        Text(liveDone > 0 ? "Resume (\(liveDone)/\(batch.totalFiles))" : "Save \(batch.totalFiles)")
                            .font(.subheadline.weight(.semibold))
                            .padding(.horizontal, 12)
                            .padding(.vertical, 7)
                            .background(Color.accentColor, in: Capsule())
                            .foregroundStyle(.white)
                    }
                    .buttonStyle(.plain)
                }
            }

            ProgressView(value: Double(liveDone), total: Double(max(1, batch.totalFiles)))
                .tint(isComplete ? .green : .blue)
        }
        .padding(.vertical, 4)
    }
}
