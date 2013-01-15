/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.xbib.jdbc.csv;

import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 * Performs string matching for SQL LIKE patterns.
 */
public class LikePattern {

    /**
     * Global lookup table of LIKE pattern to compiled regular expression.
     */
    private static Hashtable<String, Pattern> compiledRegexs = new Hashtable<String, Pattern>();

    /**
     * @param likePattern an SQL LIKE pattern including % and _ characters.
     * @param input       string to be matched.
     * @return true if input string matches LIKE pattern.
     */
    public static boolean matches(String likePattern, CharSequence input) {
        boolean retval;
        int percentIndex = likePattern.indexOf('%');
        int underscoreIndex = likePattern.indexOf('_');
        if (percentIndex < 0 && underscoreIndex < 0) {            /*
			 * No wildcards in pattern so we can just compare strings.
			 */
            retval = likePattern.equals(input);
        } else {
            Pattern p = compiledRegexs.get(likePattern);
            if (p == null) {
				/*
				 * First convert LIKE pattern to a regular expression.
				 */
                StringBuffer regex = new StringBuffer();
                StringTokenizer tokenizer = new StringTokenizer(likePattern, "%_", true);
                while (tokenizer.hasMoreTokens()) {
                    String token = tokenizer.nextToken();
                    if (token.equals("%")) {
                        regex.append(".*");
                    } else if (token.equals("_")) {
                        regex.append(".");
                    } else {
                        regex.append(Pattern.quote(token));
                    }
                }

				/*
				 * Cache compiled regular expression because we will probably be
				 * using the same one again and again.
				 */
                p = Pattern.compile(regex.toString());
                compiledRegexs.put(likePattern, p);
            }
            retval = p.matcher(input).matches();
        }
        return retval;
    }
}
