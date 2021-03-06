/* Generated By:JavaCC: Do not edit this line. ExpressionParserConstants.java */
package org.xbib.jdbc.csv;


/**
 * Token literal values and constants.
 * Generated by org.javacc.parser.OtherFilesGen#start()
 */
public interface ExpressionParserConstants {

    /**
     * End of File.
     */
    int EOF = 0;
    /**
     * RegularExpression Id.
     */
    int SELECT = 5;
    /**
     * RegularExpression Id.
     */
    int DISTINCT = 6;
    /**
     * RegularExpression Id.
     */
    int COMMA = 7;
    /**
     * RegularExpression Id.
     */
    int UNSIGNEDINT = 8;
    /**
     * RegularExpression Id.
     */
    int UNSIGNEDNUMBER = 9;
    /**
     * RegularExpression Id.
     */
    int DIGITS = 10;
    /**
     * RegularExpression Id.
     */
    int EXPONENT = 11;
    /**
     * RegularExpression Id.
     */
    int TYPESUFFIX = 12;
    /**
     * RegularExpression Id.
     */
    int NULL = 13;
    /**
     * RegularExpression Id.
     */
    int CURRENT_DATE = 14;
    /**
     * RegularExpression Id.
     */
    int AND = 15;
    /**
     * RegularExpression Id.
     */
    int OR = 16;
    /**
     * RegularExpression Id.
     */
    int NOT = 17;
    /**
     * RegularExpression Id.
     */
    int IS = 18;
    /**
     * RegularExpression Id.
     */
    int AS = 19;
    /**
     * RegularExpression Id.
     */
    int LIKE = 20;
    /**
     * RegularExpression Id.
     */
    int BETWEEN = 21;
    /**
     * RegularExpression Id.
     */
    int PLACEHOLDER = 22;
    /**
     * RegularExpression Id.
     */
    int ASC = 23;
    /**
     * RegularExpression Id.
     */
    int DESC = 24;
    /**
     * RegularExpression Id.
     */
    int LOWER = 25;
    /**
     * RegularExpression Id.
     */
    int ROUND = 26;
    /**
     * RegularExpression Id.
     */
    int UPPER = 27;
    /**
     * RegularExpression Id.
     */
    int COUNT = 28;
    /**
     * RegularExpression Id.
     */
    int MAX = 29;
    /**
     * RegularExpression Id.
     */
    int MIN = 30;
    /**
     * RegularExpression Id.
     */
    int SUM = 31;
    /**
     * RegularExpression Id.
     */
    int AVG = 32;
    /**
     * RegularExpression Id.
     */
    int FROM = 33;
    /**
     * RegularExpression Id.
     */
    int WHERE = 34;
    /**
     * RegularExpression Id.
     */
    int GROUP = 35;
    /**
     * RegularExpression Id.
     */
    int ORDER = 36;
    /**
     * RegularExpression Id.
     */
    int BY = 37;
    /**
     * RegularExpression Id.
     */
    int HAVING = 38;
    /**
     * RegularExpression Id.
     */
    int LIMIT = 39;
    /**
     * RegularExpression Id.
     */
    int OFFSET = 40;
    /**
     * RegularExpression Id.
     */
    int NAME = 41;
    /**
     * RegularExpression Id.
     */
    int STRING = 42;
    /**
     * RegularExpression Id.
     */
    int RELOP = 43;
    /**
     * RegularExpression Id.
     */
    int ASTERISK = 44;
    /**
     * RegularExpression Id.
     */
    int NAMEASTERISK = 45;
    /**
     * RegularExpression Id.
     */
    int MINUS = 46;
    /**
     * RegularExpression Id.
     */
    int PLUS = 47;
    /**
     * RegularExpression Id.
     */
    int DIVIDE = 48;
    /**
     * RegularExpression Id.
     */
    int OPENPARENTHESIS = 49;
    /**
     * RegularExpression Id.
     */
    int CLOSEPARENTHESIS = 50;
    /**
     * RegularExpression Id.
     */
    int SEMICOLON = 51;
    /**
     * RegularExpression Id.
     */
    int TABLENAME = 56;
    /**
     * RegularExpression Id.
     */
    int QUOTEDTABLENAME = 57;
    /**
     * RegularExpression Id.
     */
    int UNEXPECTED_CHAR = 58;

    /**
     * Lexical state.
     */
    int DEFAULT = 0;
    /**
     * Lexical state.
     */
    int IN_TABLE = 1;

    /**
     * Literal token values.
     */
    String[] tokenImage = {
            "<EOF>",
            "\" \"",
            "\"\\t\"",
            "\"\\r\"",
            "\"\\n\"",
            "\"SELECT\"",
            "\"DISTINCT\"",
            "\",\"",
            "<UNSIGNEDINT>",
            "<UNSIGNEDNUMBER>",
            "<DIGITS>",
            "<EXPONENT>",
            "\"L\"",
            "\"NULL\"",
            "\"CURRENT_DATE\"",
            "\"AND\"",
            "\"OR\"",
            "\"NOT\"",
            "\"IS\"",
            "\"AS\"",
            "\"LIKE\"",
            "\"BETWEEN\"",
            "\"?\"",
            "\"ASC\"",
            "\"DESC\"",
            "\"LOWER\"",
            "\"ROUND\"",
            "\"UPPER\"",
            "\"COUNT\"",
            "\"MAX\"",
            "\"MIN\"",
            "\"SUM\"",
            "\"AVG\"",
            "\"FROM\"",
            "\"WHERE\"",
            "\"GROUP\"",
            "\"ORDER\"",
            "\"BY\"",
            "\"HAVING\"",
            "\"LIMIT\"",
            "\"OFFSET\"",
            "<NAME>",
            "<STRING>",
            "<RELOP>",
            "\"*\"",
            "<NAMEASTERISK>",
            "\"-\"",
            "\"+\"",
            "\"/\"",
            "\"(\"",
            "\")\"",
            "\";\"",
            "\" \"",
            "\"\\t\"",
            "\"\\r\"",
            "\"\\n\"",
            "<TABLENAME>",
            "<QUOTEDTABLENAME>",
            "<UNEXPECTED_CHAR>",
    };

}
