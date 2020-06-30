import java.io.File;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

public class Davisbase {
    static String prompt = "davisql>";
    static String version = "v1.0";
    static String copyRight = "Copyright \\u00a9 2020 - Preethi Kesavan";
    static boolean isExit = false;
    static long pageSize = 512;
    static Scanner scanner = new Scanner(System.in).useDelimiter(";");
    static String dbDirPath = "./Data";

    public static String getPrompt() {
        return prompt;
    }

    public static String getVersion() {
        return version;
    }

    public static String getCopyRight() {
        return copyRight;
    }

    public static long getPageSize(){
        return pageSize;
    }

    private static void showVersion(){
        System.out.println("Version is " + getVersion());
    }

    private static String displayLine(char ch, int count){
        String lineStr = "";
        for(int i=0; i<count; i++){
            lineStr += ch;
        }
        return lineStr;
    }

    public static void displayWelcomeScreen(){
        System.out.println(displayLine('-', 80));
        System.out.println("Welcome to DavisBaseLite !");
        System.out.println("DavisBaseLite Version " + getVersion());
        System.out.println(getCopyRight());
        System.out.println("\nType \"help;\" to display supported commands");
        System.out.println(displayLine('-', 80));
    }

    private static void printCmd(String str){
        System.out.println("\n\t" + str + "\n");
    }

    private static void printDef(String str){
        System.out.println("\t\t" + str);
    }

    public static void showHelp(){
        System.out.println(displayLine('*',80));
        System.out.println("Help Screen");
        System.out.println("Supported Commands (All commands below are case insensitive)");
        printCmd("SHOW TABLES;");
        printDef("To display the names of all tables.");
        printCmd("CREATE TABLE <table_name>");
        printCmd("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
        printDef("To display table records which meets optional <condition> is <column_name> = <value>.");
        printCmd("DROP TABLE <table_name>;");
        printDef("To remove table data and its schema.");
        printCmd("VERSION;");
        printDef("To display the version of the program.");
        printCmd("HELP;");
        printDef("To display the Help screen.");
        printCmd("EXIT;");
        printDef("Exit the program.");
        System.out.println(displayLine('*',80));
    }

    public static void showError(String userCmd){
        System.out.println("Command is incorrect ");
        System.out.println(userCmd);
        System.out.println("Please check and try again.");
    }

    public static boolean isTablePresent(String tableName, boolean showMessage) {
        try {
            File file = new File(dbDirPath + "/" + tableName + ".tbl");
            if ((file.exists()) && (!file.isDirectory()))
                return true;
            if (showMessage) {
                System.out.println("Table " + tableName + " is not present");
            }
        } catch (Exception e) {
            return false;
        }

        return false;
    }

    public static long convertStringToDate(String dateString) {
        String pattern = "MM:dd:yyyy";
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        try {
            Date date = format.parse(dateString);
            return date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new Date().getTime();
    }

    public static String convertDateToString(long date) {
        String pattern = "MM:dd:yyyy";
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        Date d = new Date(date);
        return format.format(d);
    }

    public static String convertDateTimeToString(long date) {
        String pattern = "YYYY-MM-DD_hh:mm:ss";
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        Date d = new Date(date);
        return format.format(d);
    }

    public static void main(String[] args) {
        displayWelcomeScreen();
        String userCommand = "";
        while (!isExit) {
            System.out.print(getPrompt());
            userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
            parseUserCommand(userCommand);
        }
        System.out.println("Exiting...");
    }

    private static void parseUserCommand(String userCommand) {
        ArrayList<String> commandTokens = new ArrayList<String>(java.util.Arrays.asList(userCommand.split(" ")));
        String str = commandTokens.get(0);
        switch (str) {
            case "create":
                createTable(userCommand);
                break;

            case "insert":
                insertRecord(userCommand);
                break;

            case "select":
                if (userCommand.contains("where"))
                    selectWithWhere(userCommand);
                else
                    selectRow(userCommand);
                break;

            case "drop":
                dropTable(userCommand);
                break;

            case "exit":
                System.exit(0);
                break;

            case "help":
                showHelp();
                break;

            case "quit":
                System.exit(0);
                break;

            case "show":
                showTables(userCommand);
                break;

            case "version":
                System.out.println(getVersion());
                break;
        }
    }

    public static void showTables(String userCommand) {
        try {
            RandomAccessFile database = new RandomAccessFile(dbDirPath + "/davisbase_tables.tbl", "rw");
            boolean isRecordPresent = false;
            while (database.getFilePointer() < database.length()) {
                int isDeleted = database.readByte();
                byte length = database.readByte();
                byte[] bytes = new byte[length];
                database.read(bytes, 0, bytes.length);
                if (isDeleted == 0) {
                    isRecordPresent = true;
                    System.out.println(new String(bytes));
                }
                database.readInt();
            }
            if (!isRecordPresent)
                System.out.println("No such table is present");
            database.close();
        } catch (Exception e) {
            System.out.println("Error, while fetching values from database");
        }
    }

    public static String[] getTokens(String userCommand) {
        userCommand = userCommand.replace('(', '#').replace(',', '#').replace(')', ' ').trim();
        return userCommand.split("#");
    }

    public static void createTable(String userCommand) {
        try {
            String[] tokens = getTokens(userCommand);
            String tableName = tokens[0].trim().split(" ")[2];
            if (!isTablePresent(tableName, false)) {
                tokens[0] = "rowid int";
                RandomAccessFile tables = new RandomAccessFile(dbDirPath + "/davisbase_tables.tbl", "rw");
                tables.seek(tables.length());
                tables.writeByte(0);
                tables.writeByte(tableName.length());
                tables.writeBytes(tableName);
                tables.writeInt(0);
                tables.close();

                RandomAccessFile columns = new RandomAccessFile(dbDirPath + "/davisbase_columns.tbl", "rw");
                columns.seek(columns.length());
                for (String token : tokens) {
                    token = token.trim();
                    if ((token != null) && (!token.isEmpty())) {
                        columns.writeByte(0);
                        if (token.contains("primary key")) {
                            token = token.replace("primary key", "primarykey");
                        }
                        if (token.contains("not nullable")) {
                            token = token.replace("not nullable", "notnullable");
                        }
                        String columnDefination = tableName + "#"
                                + token.replaceAll("  ", " ").replaceAll(" ", "#").trim();
                        columns.writeByte(columnDefination.length());
                        columns.writeBytes(columnDefination);
                    }
                }
                columns.close();
                RandomAccessFile table = new RandomAccessFile(dbDirPath + "/" + tableName + ".tbl", "rw");
                table.close();
                System.out.println("Record is created Successfully");
            } else {
                System.out.println("Table is already created");
            }
        } catch (Exception e) {
            System.out.println("Error, while Creating a table");
        }
    }

    public static void insertRecord(String userCommand) {
        try {
            String[] tokens = getTokens(userCommand);
            String tableName = tokens[0].trim().split(" ")[2];
            List<TableColumn> columns = getColumns(tableName);

            int rows = 0;
            RandomAccessFile databases = new RandomAccessFile(dbDirPath + "/davisbase_tables.tbl", "rw");
            long pos = -1L;
            while (databases.getFilePointer() < databases.length()) {
                databases.readByte();
                byte length = databases.readByte();
                byte[] bytes = new byte[length];
                databases.read(bytes, 0, bytes.length);
                String databaseTableName = new String(bytes);
                pos = databases.getFilePointer();
                rows = databases.readInt();
                if (databaseTableName.equals(tableName)) {
                    rows++;
                    break;
                }
            }

            tokens[1] = (rows + "," + tokens[1]);
            String[] values = tokens[1].trim().split(",");

            int recordSize = 0;
            boolean isError = false;
            if (columns.size() == values.length) {
                for (int i = 0; i < values.length; i++) {
                    if ((((TableColumn) columns.get(i)).isNotNullable()) || (((TableColumn) columns.get(i)).isPrimary())) {
                        if ((values[i] == null) || (values[i] == "null")) {
                            isError = true;
                        }
                        if (((TableColumn) columns.get(i)).isPrimary()) {
                            isError = isKeyAlreadyPresent("select * from " + tableName + " where "
                                    + ((TableColumn) columns.get(i)).getColumnName() + "=" + values[i]);
                        }
                        if (isError)
                            break;
                    }
                    if (((TableColumn) columns.get(i)).getDataType().equals("int")) {
                        recordSize += 4;
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("tinyint")) {
                        recordSize++;
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("smallint")) {
                        recordSize += 2;
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("bigint")) {
                        recordSize += 8;
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("real")) {
                        recordSize += 4;
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("double")) {
                        recordSize += 8;
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("date")) {
                        recordSize += 8;
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("datetime")) {
                        recordSize += 8;
                    } else {
                        recordSize += values[i].length() + 1;
                    }
                }
            }

            if (!isError) {
                databases.seek(pos);
                databases.writeInt(rows);
                BTree btree = new BTree();
                btree.tableName = dbDirPath + "/" + tableName + ".tbl";
                long pointer = btree.insert(recordSize);
                RandomAccessFile table = new RandomAccessFile(btree.tableName, "rw");
                table.seek(pointer);
                for (int i = 0; i < values.length; i++) {
                    if (((TableColumn) columns.get(i)).getDataType().equals("int")) {
                        table.writeInt(Integer.parseInt(values[i]));
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("tinyint")) {
                        table.writeByte(Byte.parseByte(values[i]));
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("smallint")) {
                        table.writeInt(Short.parseShort(values[i]));
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("bigint")) {
                        table.writeLong(Long.parseLong(values[i]));
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("real")) {
                        table.writeFloat(Float.parseFloat(values[i]));
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("double")) {
                        table.writeDouble(Double.parseDouble(values[i]));
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("date")) {
                        table.writeLong(convertStringToDate(values[i]));
                    } else if (((TableColumn) columns.get(i)).getDataType().equals("datetime")) {
                        table.writeLong(Long.parseLong(values[i]));
                    } else {
                        table.writeByte(values[i].length());
                        table.writeBytes(values[i]);
                    }
                }
                table.close();
                System.out.println("Record is inserted Successfully");
            } else {
                System.out.println("Primary key should be unique");
                System.out.println("or");
                System.out.println("Nullable Field can't be null");
            }
            databases.close();
        } catch (Exception e) {
            System.out.println("Error, while inserting a record");
        }
    }

    public static List<TableColumn> getColumns(String tableName) {
        List<TableColumn> columns = new java.util.ArrayList<TableColumn>();
        try {
            if (isTablePresent(tableName, true)) {
                RandomAccessFile table = new RandomAccessFile(dbDirPath + "/davisbase_columns.tbl", "rw");
                while (table.getFilePointer() < table.length()) {
                    int isDeleted = table.readByte();
                    byte length = table.readByte();
                    byte[] bytes = new byte[length];
                    table.read(bytes, 0, bytes.length);
                    String[] column = new String(bytes).replaceAll("#", " ").split(" ");
                    if ((column[0].equals(tableName)) && (isDeleted == 0)) {
                        TableColumn c = new TableColumn();
                        c.setColumnName(column[1]);
                        c.setDataType(column[2]);
                        c.setPrimary(false);
                        c.setNotNullable(false);
                        if (column.length == 4) {
                            if (column[3].equals("primarykey")) {
                                c.setPrimary(true);
                            } else if (column[3].equals("notnullable")) {
                                c.setNotNullable(true);
                            }
                        }
                        columns.add(c);
                    }
                }
                table.close();
            }
        } catch (Exception e) {
            System.out.println("Error");
        }

        return columns;
    }

    public static void selectRow(String userCommand) {
        try {
            String[] tokens = userCommand.split(" ");
            String tableName = tokens[3].trim();
            if (isTablePresent(tableName, true)) {
                RandomAccessFile table = new RandomAccessFile(dbDirPath + "/" + tableName + ".tbl", "rw");
                if (table.length() > 0L) {
                    List<TableColumn> columns = getColumns(tableName);
                    table.readByte();
                    int cells = table.readByte();
                    table.readShort();
                    long rightPointer = table.readInt();
                    ArrayList<Short> cellPointers = new ArrayList<Short>();
                    for (int i = 0; i < cells; i++) {
                        cellPointers.add(Short.valueOf(table.readShort()));
                    }
                    boolean nextPage = true;
                    while (nextPage) {
                        for (int i = 0; i < cellPointers.size(); i++) {
                            table.seek(((Short) cellPointers.get(i)).shortValue());
                            for (TableColumn column : columns) {
                                if (column.getDataType().equals("int")) {
                                    System.out.print(" " + table.readInt());
                                } else if (column.getDataType().equals("tinyint")) {
                                    System.out.print(" " + table.readByte());
                                } else if (column.getDataType().equals("smallint")) {
                                    System.out.print(" " + table.readShort());
                                } else if (column.getDataType().equals("bigint")) {
                                    System.out.print(" " + table.readLong());
                                } else if (column.getDataType().equals("real")) {
                                    System.out.print(" " + table.readFloat());
                                } else if (column.getDataType().equals("double")) {
                                    System.out.print(" " + table.readDouble());
                                } else if (column.getDataType().equals("date")) {
                                    System.out.print(" " + convertDateToString(table.readLong()));
                                } else if (column.getDataType().equals("datetime")) {
                                    System.out.print(" " + convertDateTimeToString(table.readLong()));
                                } else {
                                    int length = table.readByte();
                                    byte[] bytes = new byte[length];
                                    table.read(bytes, 0, bytes.length);
                                    System.out.print(" " + new String(bytes));
                                }
                            }
                            System.out.println();
                        }
                        if (rightPointer != 0L) {
                            table.seek(rightPointer);
                            table.readByte();
                            cells = table.readByte();
                            table.readShort();
                            rightPointer = table.readInt();
                            cellPointers = new ArrayList<Short>();
                            for (int i = 0; i < cells; i++) {
                                cellPointers.add(Short.valueOf(table.readShort()));
                            }
                        } else {
                            nextPage = false;
                        }
                    }
                } else {
                    System.out.println("No record present");
                }
                table.close();
            }
        } catch (Exception e) {
            System.out.println("Error, While fectching records from table");
        }
    }

    public static void selectWithWhere(String userCommand) {
        try {
            String[] tokens = userCommand.split(" ");
            String tableName = tokens[3].trim();
            if (isTablePresent(tableName, true)) {
                String filter = userCommand.substring(userCommand.indexOf("where") + 5, userCommand.length()).trim();
                String[] filterArray = filter.split("=");
                RandomAccessFile table = new RandomAccessFile(dbDirPath + "/" + tableName + ".tbl", "rw");
                if (table.length() > 0L) {
                    List<TableColumn> columns = getColumns(tableName);

                    table.readByte();
                    int cells = table.readByte();
                    table.readShort();
                    long rightPointer = table.readInt();
                    ArrayList<Short> cellPointers = new ArrayList<Short>();
                    for (int i = 0; i < cells; i++) {
                        cellPointers.add(Short.valueOf(table.readShort()));
                    }
                    boolean nextPage = true;
                    while (nextPage) {
                        for (int i = 0; i < cellPointers.size(); i++) {
                            table.seek(((Short) cellPointers.get(i)).shortValue());
                            String displayString = "";
                            Boolean isDisplay = Boolean.valueOf(false);
                            for (TableColumn column : columns) {
                                if (column.getDataType().equals("int")) {
                                    String value = "" + table.readInt();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        isDisplay = Boolean.valueOf(true);
                                    }

                                    displayString = displayString + " " + value;
                                } else if (column.getDataType().equals("tinyint")) {
                                    String value = "" + table.readByte();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        isDisplay = Boolean.valueOf(true);
                                    }

                                    displayString = displayString + " " + value;
                                } else if (column.getDataType().equals("smallint")) {
                                    String value = "" + table.readShort();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        isDisplay = Boolean.valueOf(true);
                                    }

                                    displayString = displayString + " " + value;
                                } else if (column.getDataType().equals("bigint")) {
                                    String value = "" + table.readLong();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        isDisplay = Boolean.valueOf(true);
                                    }

                                    displayString = displayString + " " + value;
                                } else if (column.getDataType().equals("real")) {
                                    String value = "" + table.readFloat();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        isDisplay = Boolean.valueOf(true);
                                    }

                                    displayString = displayString + " " + value;
                                } else if (column.getDataType().equals("double")) {
                                    String value = "" + table.readDouble();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        isDisplay = Boolean.valueOf(true);
                                    }

                                    displayString = displayString + " " + value;
                                } else if (column.getDataType().equals("date")) {
                                    String value = convertDateToString(table.readLong());
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        isDisplay = Boolean.valueOf(true);
                                    }

                                    displayString = displayString + " " + value;
                                } else if (column.getDataType().equals("datetime")) {
                                    String value = convertDateTimeToString(table.readLong());
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        isDisplay = Boolean.valueOf(true);
                                    }

                                    displayString = displayString + " " + value;
                                } else {
                                    int length = table.readByte();
                                    byte[] bytes = new byte[length];
                                    table.read(bytes, 0, bytes.length);
                                    String value = " " + new String(bytes);
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        isDisplay = Boolean.valueOf(true);
                                    }

                                    displayString = displayString + " " + value;
                                }
                                if (isDisplay.booleanValue())
                                    System.out.println(displayString);
                            }
                        }
                        if (rightPointer != 0L) {
                            table.seek(rightPointer);
                            table.readByte();
                            cells = table.readByte();
                            table.readShort();
                            rightPointer = table.readInt();
                            cellPointers = new ArrayList<Short>();
                            for (int i = 0; i < cells; i++) {
                                cellPointers.add(Short.valueOf(table.readShort()));
                            }
                        } else {
                            nextPage = false;
                        }
                    }
                } else {
                    System.out.println("No record present");
                }
                table.close();
            }
        } catch (Exception e) {
            System.out.println("Error, While fectching records from table");
        }
    }

    public static boolean isKeyAlreadyPresent(String userCommand) {
        try {
            String[] tokens = userCommand.split(" ");
            String tableName = tokens[3].trim();
            if (isTablePresent(tableName, true)) {
                String filter = userCommand.substring(userCommand.indexOf("where") + 5, userCommand.length()).trim();
                String[] filterArray = filter.split("=");
                RandomAccessFile table = new RandomAccessFile(dbDirPath + "/" + tableName + ".tbl",
                        "rw");
                if (table.length() > 0L) {
                    java.util.List<TableColumn> columns = getColumns(tableName);

                    table.readByte();
                    int cells = table.readByte();
                    table.readShort();
                    long rightPointer = table.readInt();
                    ArrayList<Short> cellPointers = new ArrayList();
                    for (int i = 0; i < cells; i++) {
                        cellPointers.add(Short.valueOf(table.readShort()));
                    }
                    boolean nextPage = true;
                    while (nextPage) {
                        for (int i = 0; i < cellPointers.size(); i++) {
                            table.seek(((Short) cellPointers.get(i)).shortValue());

                            for (TableColumn column : columns) {
                                if (column.getDataType().equals("int")) {
                                    String value = "" + table.readInt();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("tinyint")) {
                                    String value = "" + table.readByte();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("smallint")) {
                                    String value = "" + table.readShort();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("bigint")) {
                                    String value = "" + table.readLong();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("real")) {
                                    String value = "" + table.readFloat();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("double")) {
                                    String value = "" + table.readDouble();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("date")) {
                                    String value = convertDateToString(table.readLong());
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("datetime")) {
                                    String value = convertDateTimeToString(table.readLong());
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }
                                } else {
                                    int length = table.readByte();
                                    byte[] bytes = new byte[length];
                                    table.read(bytes, 0, bytes.length);
                                    String value = " " + new String(bytes);
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }
                                }
                            }
                        }

                        if (rightPointer != 0L) {
                            table.seek(rightPointer);
                            table.readByte();
                            cells = table.readByte();
                            table.readShort();
                            rightPointer = table.readInt();
                            cellPointers = new ArrayList<Short>();
                            for (int i = 0; i < cells; i++) {
                                cellPointers.add(Short.valueOf(table.readShort()));
                            }
                        } else {
                            nextPage = false;
                        }
                    }
                }
                table.close();
            }
        } catch (Exception e) {
            System.out.println("Error, While fectching records from table");
        }

        return false;
    }

    public static void dropTable(String userCommand) {
        try {
            String[] tokens = userCommand.split(" ");
            String tableName = tokens[2];

            RandomAccessFile database = new RandomAccessFile(dbDirPath + "/davisbase_tables.tbl", "rw");
            while (database.getFilePointer() < database.length()) {
                int isDeleted = database.readByte();
                byte length = database.readByte();
                byte[] bytes = new byte[length];
                database.read(bytes, 0, bytes.length);
                if ((tableName.equals(new String(bytes))) && (isDeleted == 0)) {
                    database.seek(database.getFilePointer() - length - 2L);
                    database.writeByte(1);
                    break;
                }
                database.readInt();
            }
            database.close();
            markAllColumnsDeleted(tableName);
            File file = new File(dbDirPath + "/" + tableName + ".tbl");
            file.delete();
            System.out.println("Record is deleted Successfully");
        } catch (Exception e) {
            System.out.println("Error, while dropping a table");
        }
    }

    public static void markAllColumnsDeleted(String tableName) {
        try {
            if (isTablePresent(tableName, true)) {
                RandomAccessFile table = new RandomAccessFile(dbDirPath + "/davisbase_columns.tbl", "rw");
                while (table.getFilePointer() < table.length()) {
                    int isDeleted = table.readByte();
                    byte length = table.readByte();
                    byte[] bytes = new byte[length];
                    table.read(bytes, 0, bytes.length);
                    String[] column = new String(bytes).replaceAll("#", " ").split(" ");
                    if ((column[0].equals(tableName)) && (isDeleted == 0)) {
                        long tablePointer = table.getFilePointer();
                        table.seek(tablePointer - length - 1L);
                        table.writeByte(1);
                        table.seek(tablePointer);
                    }
                }
                table.close();
            }
        } catch (Exception e) {
            System.out.println("Error");
        }
    }
}
