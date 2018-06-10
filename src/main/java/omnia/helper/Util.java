package omnia.helper;

import joptsimple.internal.Strings;

import java.time.LocalDateTime;
import java.util.function.BiPredicate;

public class Util {

    public static double getDoubleValueOrDefault(Object obj) {
        if(obj == null) {
            return 0.0;
        } else {
            return (double) obj;
        }
    }

    public static int getIntValueOrDefault(Object obj) {
        if(obj == null) {
            return 0;
        } else {
            return (int) obj;
        }
    }

    public static String getDateComparison(Object obj1, Object obj2, BiPredicate<LocalDateTime, LocalDateTime> predicate) {
        LocalDateTime date1 = obj1 == null ? null : LocalDateTime.parse(obj1.toString());
        LocalDateTime date2 = obj2 == null ? null : LocalDateTime.parse(obj2.toString());
        if(date1 == null && date2 == null) {
            return Strings.EMPTY;
        } else if(date1 == null) {
            return obj2.toString();
        } else if(date2 == null) {
            return obj1.toString();
        } else if(predicate.test(date1, date2)) {
            return obj1.toString();
        } else {
            return obj2.toString();
        }
    }

}
