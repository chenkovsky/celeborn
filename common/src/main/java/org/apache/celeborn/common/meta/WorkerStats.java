package org.apache.celeborn.common.meta;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class WorkerStats {

    private final Map<String, String> stats;

    public WorkerStats(Map<String, String> stats) {
        this.stats = stats;
    }

    public Map<String, String> getStats() {
        return stats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkerStats)) return false;
        WorkerStats that = (WorkerStats) o;
        return Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(stats);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkerStats{");
        for (Map.Entry<String, String> entry: this.stats.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(", ");
        }
        if (!this.stats.isEmpty()) {
            sb.delete(sb.length() - 2, sb.length());
        }
        sb.append("}");
        return sb.toString();
    }

    public static WorkerStats defaultWorkerStats() {
        return new WorkerStats(new HashMap<>());
    }
}
