package io.netty.example.handler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class GlobPatternTest {

    @ParameterizedTest(name = "\"{0}\" vs \"{1}\" -> {2}")
    @CsvSource(delimiter = '|', value = {
            "abc      | abc      | true",
            "abc      | abd      | false",
            "abc      | ab       | false",
            "ab       | abc      | false",
            "''       | ''       | true",
            "''       | a        | false",
            "a        | ''       | false",
            "*        | anything | true",
            "*        | ''       | false",
            "a*       | a        | true",
            "a*c      | abbc     | true",
            "a*c      | abbd     | false",
            "*a*b*    | xaxxbx   | true",
            "*a*b*    | xbxxax   | false",
            "a**b     | ab       | true",
            "**       | x        | true",
            "?        | a        | true",
            "?        | ''       | false",
            "??       | a        | false",
            "a?c      | abc      | true",
            "a?c      | ac       | false",
            "[abc]    | b        | true",
            "[abc]    | d        | false",
            "[abc]    | ab       | false",
            "[^abc]   | d        | true",
            "[^abc]   | a        | false",
            "[a-c]    | b        | true",
            "[a-c]    | d        | false",
            "[c-a]    | b        | true",
            "[c-a]    | d        | false",
            "\\*      | *        | true",
            "\\*      | a        | false",
            "\\?      | ?        | true",
            "\\?      | a        | false",
            "\\[a]    | [a]      | true",
            "\\[a]    | a        | false",
            "[\\]]    | ]        | true",
            "[\\*]    | *        | true",
            "[\\*]    | a        | false",
            "a\\      | a\\      | true",
            "[abc     | a        | true",
            "[abc     | ab       | false",
            "[abc     | d        | false",
            "abc*     | abc      | true",
            "abc**    | abc      | true",
            "abc*d    | abc      | false",
    })
    void matchesLikeRedis(String pattern, String string, boolean expected) {
        assertEquals(expected, GlobPattern.matches(pattern, string));
    }

    @Test
    void patternNestedDeeperThanLimitDoesNotMatch() {
        final String pattern = "*a".repeat(1002);
        final String string = "a".repeat(1002);
        assertFalse(GlobPattern.matches(pattern, string));
    }
}
