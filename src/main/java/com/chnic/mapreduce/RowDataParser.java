package com.chnic.mapreduce;

import java.util.AbstractMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RowDataParser {

    private static final Pattern pattern = Pattern.compile("^.{15}(\\d{4}).{68}([+-]\\d{4})(\\d).*$");

    private static final int MISSING = 9999;

    public static Map.Entry<Integer, Float> parser(String line) throws ValidateException {
        Matcher matcher = pattern.matcher(line);
        if (!matcher.matches()) {
            throw new ValidateException(ValidateResult.MALFORMED.name());
        }

        int year = Integer.parseInt(matcher.group(1));
        int temperature = Integer.parseInt(matcher.group(2));
        String qualityCode = matcher.group(3);

        if (temperature == MISSING || qualityCode.matches("[^01459]")) {
            throw new ValidateException(ValidateResult.MISSING.name());
        }

        return new AbstractMap.SimpleEntry<>(year, (float) (temperature / 10.0));
    }
}
