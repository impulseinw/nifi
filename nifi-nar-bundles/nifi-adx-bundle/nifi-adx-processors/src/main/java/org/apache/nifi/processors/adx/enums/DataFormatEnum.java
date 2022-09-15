package org.apache.nifi.processors.adx.enums;

public enum DataFormatEnum {
    AVRO(".avro", "An Avro format with support for logical types and for the snappy compression codec."),
    APACHEAVRO(".apacheavro", "An Avro format with support for logical types and for the snappy compression codec."),
    CSV(".csv", "A text file with comma-separated values (,). For more information, see RFC 4180: Common Format " +
            "and MIME Type for Comma-Separated Values (CSV) Files."),
    JSON(".json", "A text file containing JSON objects separated by \\n or \\r\\n. For more information, " +
            "see JSON Lines (JSONL)."),
    MULTIJSON(".multijson", "A text file containing a JSON array of property containers (each representing a record) or any " +
            "number of property containers separated by spaces, \\n or \\r\\n. Each property container may be " +
            "spread across multiple lines. This format is preferable to JSON unless the data is not property " +
            "containers."),
    ORC(".orc", "An ORC file."),
    PARQUET(".parquet", "A parquet file."),
    PSV(".psv", "A text file with values separated by vertical bars (|)."),
    SCSV(".scsv", "A text file with values separated by semicolons (;)."),
    SOHSV(".sohsv", "A text file with SOH-separated values. (SOH is the ASCII code point 1. " +
            "This format is used by Hive in HDInsight)."),
    TSV(".tsv", "A text file with tab delimited values (\\t)."),
    TSVE(".tsv", "A text file with tab-delimited values (\\t). A backslash (\\) is used as escape character."),
    TXT(".txt", "A text file with lines separated by \\n. Empty lines are skipped.");

    DataFormatEnum(String extension, String description) {
        this.extension = extension;
        this.description = description;
    }

    private final String extension;

    private final String description;

    public String getExtension() {
        return extension;
    }

    public String getDescription(){
        return description;
    }


}
