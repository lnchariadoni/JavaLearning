package org.example.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class Utils {
    /**
     * Returns current timestamp in format: dd-mm-yyyy hh:mm:SS:mmm:nnnnnn
     * @return formatted current datetime string
     */
    public static String getCurrentFormattedTime() {
        // Get current time with nanosecond precision
        Instant now = Instant.now();

        // Convert to ZonedDateTime (using system default timezone)
        ZonedDateTime dateTime = now.atZone(ZoneId.systemDefault());

        // Extract components
        int day = dateTime.getDayOfMonth();
        int month = dateTime.getMonthValue();
        int year = dateTime.getYear();
        int hour = dateTime.getHour();
        int minute = dateTime.getMinute();
        int second = dateTime.getSecond();

        // Extract nanoseconds within the second (0 to 999,999,999)
        int totalNanosInSecond = dateTime.getNano();

        // Split into milliseconds (3 digits) and remaining nanoseconds (6 digits)
        int milliseconds = totalNanosInSecond / 1_000_000;
        int remainingNanos = totalNanosInSecond % 1_000_000;

        // Format as: dd mm yyyy hh:mm:SS:mmm:nnnnnn
        return String.format("%02d-%02d-%04d %02d:%02d:%02d:%03d:%06d",
                day, month, year, hour, minute, second, milliseconds, remainingNanos);
    }

    public static String constructMessage(String message) {
        return getCurrentFormattedTime() + ":" + Thread.currentThread().getName() + ":" + message;
    }
}
