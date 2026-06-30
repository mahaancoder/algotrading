package com.satyam.trading2.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Daily cleanup scheduler that runs at 9 AM to clear specific files
 * Cleans up log files and CSV files from the previous day
 */
@Slf4j
@Service
public class DailyCleanupScheduler {

    // Base directory for file cleanup
    private static final String BASE_DIR = "/home/ec2-user/";
    
    // Files to clean up daily
    private static final String[] FILES_TO_CLEAN = {
        "app.log",
        "pending_order.csv",
        "buy_signal.csv",
        "dip_states.csv"
    };

    /**
     * Runs every day at 9:00 AM
     * Cron format: second minute hour day month weekday
     * "0 0 9 * * *" = At 9:00 AM every day
     */
    @Scheduled(cron = "0 0 9 * * *")
    public void cleanupDailyFiles() {
        log.info("🧹 Starting daily cleanup at 9 AM...");
        System.out.println("🧹 ========================================");
        System.out.println("🧹 DAILY CLEANUP - " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.println("🧹 ========================================");
        
        int successCount = 0;
        int failCount = 0;
        int notFoundCount = 0;
        
        for (String fileName : FILES_TO_CLEAN) {
            try {
                Path filePath = Paths.get(BASE_DIR, fileName);
                File file = filePath.toFile();
                
                if (file.exists()) {
                    // Delete the file
                    boolean deleted = Files.deleteIfExists(filePath);
                    
                    if (deleted) {
                        log.info("✅ Deleted: {}", filePath);
                        System.out.println("✅ Deleted: " + filePath);
                        successCount++;
                    } else {
                        log.warn("⚠️ Could not delete: {}", filePath);
                        System.out.println("⚠️ Could not delete: " + filePath);
                        failCount++;
                    }
                } else {
                    log.debug("📁 File not found (skipping): {}", filePath);
                    System.out.println("📁 File not found (skipping): " + filePath);
                    notFoundCount++;
                }
            } catch (Exception e) {
                log.error("❌ Error deleting {}: {}", fileName, e.getMessage());
                System.err.println("❌ Error deleting " + fileName + ": " + e.getMessage());
                failCount++;
            }
        }
        
        // Summary
        System.out.println("🧹 ========================================");
        System.out.println("🧹 CLEANUP SUMMARY:");
        System.out.println("   ✅ Deleted: " + successCount);
        System.out.println("   ⚠️ Failed: " + failCount);
        System.out.println("   📁 Not Found: " + notFoundCount);
        System.out.println("🧹 ========================================");
        log.info("🧹 Daily cleanup completed. Deleted: {}, Failed: {}, Not Found: {}", 
                 successCount, failCount, notFoundCount);
    }
    
    /**
     * Manual cleanup trigger (can be called from API if needed)
     * This allows testing or manual cleanup outside the scheduled time
     */
    public void triggerManualCleanup() {
        log.info("🔧 Manual cleanup triggered");
        System.out.println("🔧 Manual cleanup triggered by user");
        cleanupDailyFiles();
    }
}

