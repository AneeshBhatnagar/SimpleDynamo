package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.TreeMap;

public class SimpleDynamoProvider extends ContentProvider {
    private HashMap<String, Integer> nodeMap;
    private ArrayList<String> nodeList;
    private int myPort;
    private int predecessorPort;
    private int successorPort;
    private String myHash;
    private String predecessorHash;
    private String successorHash;
    private Context context;
    private DatabaseHelper databaseHelper;
    private Uri uri;


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean onCreate() {
        context = getContext();
        databaseHelper = new DatabaseHelper(context);
        Uri.Builder builder = new Uri.Builder();
        builder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
        builder.scheme("content");
        uri = builder.build();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = Integer.parseInt(portStr);
        nodeMap = new HashMap<String, Integer>();
        nodeList = new ArrayList<String>();

        try {
            myHash = genHash(Integer.toString(myPort));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        //Building the Hash for all devices and finding own successor and predecessor
        int i = 5554;
        while (i < 5564) {
            try {
                String hash = genHash(Integer.toString(i));
                nodeMap.put(hash, i);
                nodeList.add(hash);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            i += 2;
        }
        Collections.sort(nodeList);
        int index = nodeList.indexOf(myHash);
        if (index == 0) {
            successorHash = nodeList.get(1);
            predecessorHash = nodeList.get(4);
        } else if (index == 4) {
            successorHash = nodeList.get(0);
            predecessorHash = nodeList.get(3);
        } else {
            successorHash = nodeList.get(index + 1);
            predecessorHash = nodeList.get(index - 1);
        }
        successorPort = nodeMap.get(successorHash);
        predecessorPort = nodeMap.get(predecessorHash);

        Log.d("Predecessor", Integer.toString(predecessorPort));
        Log.d("OwnPort", Integer.toString(myPort));
        Log.d("Successor", Integer.toString(successorPort));
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private int getInsertCoordinator(String hash) {
        String[] nodes = nodeList.toArray(new String[5]);
        int i = 0;
        for (; i < 4; i++) {
            if (hash.compareTo(nodes[i]) >= 0 && hash.compareTo(nodes[i + 1]) <= 0)
                break;
        }
        if (i==4){
            i = -1;
        }
        return nodeMap.get(nodes[i+1]);
    }

    private int getQueryCoordinator(String hash) {
        String[] nodes = nodeList.toArray(new String[5]);
        int i = 0;
        for (; i < 4; i++) {
            if (hash.compareTo(nodes[i]) >= 0 && hash.compareTo(nodes[i + 1]) <= 0)
                break;
        }
        if (i==4){
            i = -1;
        }
        i+=3;
        if(i>=5){
            i-=5;
        }
        return nodeMap.get(nodes[i]);
    }
}
