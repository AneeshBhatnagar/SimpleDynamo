package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/*
    Reference: https://developer.android.com/training/basics/data-storage/databases.html
 */

public class DatabaseHelper extends SQLiteOpenHelper {
    public static final int DATABASE_VERSION = 1;
    public static final String DATABASE_NAME = "SimpleDynamo.db";
    public static final String TABLE_NAME = "Dynamo";
    public static final String COLUMN_KEY = "key";
    public static final String COLUMN_VALUE = "value";

    public DatabaseHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }


    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        String SQL_CREATE_ENTRY = "CREATE TABLE " + TABLE_NAME + "(" + COLUMN_KEY + " TEXT PRIMARY KEY," + COLUMN_VALUE + " TEXT)";
        sqLiteDatabase.execSQL(SQL_CREATE_ENTRY);
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int i, int i1) {
        String SQL_DELETE_ENTRY = "DROP TABLE IF EXISTS" + TABLE_NAME;
        sqLiteDatabase.execSQL(SQL_DELETE_ENTRY);
        onCreate(sqLiteDatabase);
    }

}