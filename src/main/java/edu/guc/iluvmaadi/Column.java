package edu.guc.iluvmaadi;

public class Column {
    private String name;
    private String type;

    private String indexName;

    public Column(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getIndexName() {
        return indexName;
    }
}
