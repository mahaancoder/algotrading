package com.satyam.trading2.scheduler;

import com.satyam.trading2.service.MomentumTradingCoordinator;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MomentumTradingScheduler {
    private final MomentumTradingCoordinator coordinator;
    @Scheduled(cron="10 15 9 * * MON-FRI",zone="Asia/Kolkata") public void batch091510(){run("09:15:10",50000);}
    @Scheduled(cron="0 20 9 * * MON-FRI",zone="Asia/Kolkata") public void batch0920(){run("09:20",50000);}
    @Scheduled(cron="0 25 9 * * MON-FRI",zone="Asia/Kolkata") public void batch0925(){run("09:25",25000);}
    @Scheduled(cron="0 30 9 * * MON-FRI",zone="Asia/Kolkata") public void batch0930(){run("09:30",25000);}
    @Scheduled(cron="0 0 10 * * MON-FRI",zone="Asia/Kolkata") public void batch1000(){run("10:00",15000);}
    @Scheduled(cron="0 30 10 * * MON-FRI",zone="Asia/Kolkata") public void batch1030(){run("10:30",15000);}
    @Scheduled(cron="0 0 11 * * MON-FRI",zone="Asia/Kolkata") public void batch1100(){run("11:00",15000);}
    @Scheduled(cron="0 30 11 * * MON-FRI",zone="Asia/Kolkata") public void batch1130(){run("11:30",15000);}
    @Scheduled(cron="0 0 12 * * MON-FRI",zone="Asia/Kolkata") public void batch1200(){run("12:00",10000);}
    @Scheduled(cron="0 0 13 * * MON-FRI",zone="Asia/Kolkata") public void batch1300(){run("13:00",10000);}
    @Scheduled(cron="0 0 14 * * MON-FRI",zone="Asia/Kolkata") public void batch1400(){run("14:00",10000);}
    private void run(String slot,double budget){java.util.concurrent.CompletableFuture.runAsync(() -> coordinator.executeScheduled(slot,budget));}
}