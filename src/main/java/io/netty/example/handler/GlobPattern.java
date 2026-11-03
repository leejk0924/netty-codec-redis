package io.netty.example.handler;

// Port of stringmatchlen_impl in Redis 7.x src/util.c, case-sensitive only, over chars instead of bytes.
final class GlobPattern {
    private static final int MAX_NESTING = 1000;

    private GlobPattern() {
    }

    static boolean matches(String pattern, String string) {
        return matches(pattern, 0, string, 0, new boolean[1], 0);
    }

    private static boolean matches(String pattern, int p, String string, int s,
                                   boolean[] skipLongerMatches, int nesting) {
        if (nesting > MAX_NESTING) {
            return false;
        }
        final int pEnd = pattern.length();
        final int sEnd = string.length();

        while (p < pEnd && s < sEnd) {
            switch (pattern.charAt(p)) {
                case '*':
                    while (p + 1 < pEnd && pattern.charAt(p + 1) == '*') {
                        p++;
                    }
                    if (p + 1 == pEnd) {
                        return true;
                    }
                    while (s < sEnd) {
                        if (matches(pattern, p + 1, string, s, skipLongerMatches, nesting + 1)) {
                            return true;
                        }
                        // The rest of the pattern matched nowhere in a suffix, so letting an
                        // earlier '*' consume more of the string cannot help either.
                        if (skipLongerMatches[0]) {
                            return false;
                        }
                        s++;
                    }
                    skipLongerMatches[0] = true;
                    return false;
                case '?':
                    s++;
                    break;
                case '[': {
                    p++;
                    final boolean not = p < pEnd && pattern.charAt(p) == '^';
                    if (not) {
                        p++;
                    }
                    final char c = string.charAt(s);
                    boolean match = false;
                    while (true) {
                        if (p == pEnd) {
                            // Unterminated class: step back so the shared advance below ends the pattern.
                            p--;
                            break;
                        }
                        final char pc = pattern.charAt(p);
                        if (pc == '\\' && pEnd - p >= 2) {
                            p++;
                            if (pattern.charAt(p) == c) {
                                match = true;
                            }
                        } else if (pc == ']') {
                            break;
                        } else if (pEnd - p >= 3 && pattern.charAt(p + 1) == '-') {
                            char start = pc;
                            char end = pattern.charAt(p + 2);
                            if (start > end) {
                                final char t = start;
                                start = end;
                                end = t;
                            }
                            p += 2;
                            if (c >= start && c <= end) {
                                match = true;
                            }
                        } else if (pc == c) {
                            match = true;
                        }
                        p++;
                    }
                    if (not) {
                        match = !match;
                    }
                    if (!match) {
                        return false;
                    }
                    s++;
                    break;
                }
                case '\\':
                    if (pEnd - p >= 2) {
                        p++;
                    }
                    // fall through
                default:
                    if (pattern.charAt(p) != string.charAt(s)) {
                        return false;
                    }
                    s++;
                    break;
            }
            p++;
            if (s == sEnd) {
                while (p < pEnd && pattern.charAt(p) == '*') {
                    p++;
                }
                break;
            }
        }
        return p == pEnd && s == sEnd;
    }
}
