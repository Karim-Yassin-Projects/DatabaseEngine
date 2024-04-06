package edu.guc.iluvmaadi;

public class Column {
    private final String name;
    private final String type;

    private String indexName;

    public Column(String name, String type) {
        if (name == null || type == null) {
            throw new IllegalArgumentException("Name and type cannot be null");
        }
        if (!Types.isValidColType(type)) {
            throw new IllegalArgumentException("Invalid column type: " + type);
        }
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

    public void setIndexName(String indexName) throws DBAppException {
        if (this.indexName != null) {
            throw new DBAppException("Column "+ getName() + " already has an index");
        }
        this.indexName = indexName;
    }

}
