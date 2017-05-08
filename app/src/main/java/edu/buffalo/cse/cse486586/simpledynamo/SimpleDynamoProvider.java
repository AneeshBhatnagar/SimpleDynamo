package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static edu.buffalo.cse.cse486586.simpledynamo.DatabaseHelper.COLUMN_KEY;
import static edu.buffalo.cse.cse486586.simpledynamo.DatabaseHelper.COLUMN_VALUE;
import static edu.buffalo.cse.cse486586.simpledynamo.DatabaseHelper.TABLE_NAME;

public class SimpleDynamoProvider extends ContentProvider {
    private final int SERVER_PORT = 10000;
    private HashMap<String, Integer> nodeMap;
    private ArrayList<String> nodeList;
    private ArrayList<Integer> orderedNodes;
    private HashMap<String, ArrayList<String>> insertDeleteLog;
    private int myPort;
    private int predecessorPort;
    private int successorPort;
    private String myHash;
    private String predecessorHash;
    private String successorHash;
    private Context context;
    private DatabaseHelper databaseHelper;
    private SQLiteDatabase sqLiteDatabase;
    private Uri uri;
    private ServerSocket socket;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String hashedKey = "";
        if (selection.equals("@")) {
            sqLiteDatabase.delete(TABLE_NAME, null, null);
        } else if (selection.equals("*")) {
            sqLiteDatabase.delete(TABLE_NAME, null, null);
            MessageRequest request = new MessageRequest("Delete", Integer.toString(myPort), selection);
            try {
                String resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, request, successorPort * 2).get();
                if (resp.equals("Failure")) {
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, request, getNextNode(successorPort) * 2).get();
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } else {
            try {
                hashedKey = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            int port = getInsertCoordinator(hashedKey);
            MessageRequest request = new MessageRequest("Delete", Integer.toString(myPort), selection);
            try {
                String resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, request, port * 2).get();
                if (resp.equals("Success")) {
                    return 1;
                } else if (resp.equals("Failure")) {
                    request.setType("Delete1");
                    request.setOriginalPort(Integer.toString(port));
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, request, getNextNode(port) * 2).get();
                    return 1;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.getAsString("key"), value = values.getAsString("value");
        String hashedKey = null;
        try {
            hashedKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        int port = getInsertCoordinator(hashedKey);
        MessageRequest request = new MessageRequest("Insert", Integer.toString(myPort), key + "," + value);
        try {
            String resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, request, port * 2).get();
            if (resp.equals("Success")) {
                return uri;
            } else if (resp.equals("Failure")) {
                request.setOriginalPort(Integer.toString(port));
                request.setType("Replica1");
                port = getNextNode(port);
                String resp2 = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, request, port * 2).get();
                if (resp2.equals("Success")) {
                    return uri;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        context = getContext();
        databaseHelper = new DatabaseHelper(context);
        sqLiteDatabase = databaseHelper.getWritableDatabase();
        Uri.Builder builder = new Uri.Builder();
        builder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
        builder.scheme("content");
        uri = builder.build();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = Integer.parseInt(portStr);
        nodeMap = new HashMap<String, Integer>();
        nodeList = new ArrayList<String>();
        orderedNodes = new ArrayList<Integer>();
        insertDeleteLog = new HashMap<String, ArrayList<String>>();

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
        for (i = 0; i < nodeList.size(); i++) {
            orderedNodes.add(i, nodeMap.get(nodeList.get(i)));
        }
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


        try {
            socket = new ServerSocket(SERVER_PORT);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, socket);

        initiateRecoverySequence();

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        String[] colsFetch = {COLUMN_KEY, COLUMN_VALUE};
        String hashedKey;
        MatrixCursor matrixCursor = new MatrixCursor(colsFetch);

        if (selection.equals("@")) {
            Cursor cursor = sqLiteDatabase.rawQuery("Select * from " + TABLE_NAME, null);
            cursor.moveToFirst();

            while (!cursor.isAfterLast()) {
                Object[] values = {cursor.getString(0), cursor.getString(1)};
                matrixCursor.addRow(values);
                cursor.moveToNext();
            }
            cursor.close();

            return matrixCursor;
        } else if (selection.equals("*")) {
            try {
                MessageRequest request = new MessageRequest("Query", Integer.toString(myPort), "*");
                String response = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, request, successorPort * 2).get();
                if (response.equals("Failure")) {
                    response = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, request, getNextNode(successorPort) * 2).get();
                }
                JSONObject jsonObject = new JSONObject(response);
                JSONArray keysArray = jsonObject.getJSONArray("keys");
                JSONArray valuesArray = jsonObject.getJSONArray("values");
                for (int i = 0; i < keysArray.length(); i++) {
                    Object[] values = {keysArray.getString(i), valuesArray.getString(i)};
                    matrixCursor.addRow(values);
                }
                Cursor cursor = sqLiteDatabase.rawQuery("Select * from " + TABLE_NAME, null);
                cursor.moveToFirst();
                while (!cursor.isAfterLast()) {
                    Object[] values = {cursor.getString(0), cursor.getString(1)};
                    matrixCursor.addRow(values);
                    cursor.moveToNext();
                }
                cursor.close();
                return matrixCursor;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                e.printStackTrace();
            }
        } else {
            try {
                hashedKey = genHash(selection);
                int port = getQueryCoordinator(hashedKey);
                MessageRequest request = new MessageRequest("Query", Integer.toString(myPort), selection);
                String resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, request, port * 2).get();
                if (resp.equals("Failure")) {
                    resp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, request, getPreviousNode(port) * 2).get();
                }
                Object[] values = {selection, resp};
                matrixCursor.addRow(values);
                return matrixCursor;
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
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
        if (i == 4) {
            i = -1;
        }
        return nodeMap.get(nodes[i + 1]);
    }

    private int getQueryCoordinator(String hash) {
        String[] nodes = nodeList.toArray(new String[5]);
        int i = 0;
        for (; i < 4; i++) {
            if (hash.compareTo(nodes[i]) >= 0 && hash.compareTo(nodes[i + 1]) <= 0)
                break;
        }
        if (i == 4) {
            i = -1;
        }
        i += 3;
        if (i >= 5) {
            i -= 5;
        }
        return nodeMap.get(nodes[i]);
    }

    private int getNextNode(int search) {
        int i = orderedNodes.indexOf(search);
        if (i == orderedNodes.size() - 1) {
            i = -1;
        }
        return orderedNodes.get(i + 1);
    }

    private int getPreviousNode(int search) {
        int i = orderedNodes.indexOf(search);
        if (i == 0) {
            i = orderedNodes.size();
        }
        return orderedNodes.get(i - 1);
    }

    private synchronized void initiateRecoverySequence() {
        String message = Integer.toString(myPort) + "," + Integer.toString(predecessorPort) + "," + Integer.toString(getPreviousNode(predecessorPort));
        MessageRequest request = new MessageRequest("Recovery", Integer.toString(myPort), message);

        //Send to predecessor and successor
        String predResp = null, succResp = null;
        try {
            predResp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, request, predecessorPort * 2).get();
            succResp = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, request, successorPort * 2).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        if (predResp.equals("Failure") || succResp.equals("Failure")) {
            //Others have not started yet! So it must be the phase with no failure
            return;
        }
        String predLog = predResp, succLog = succResp;

        predLog = predLog.replaceAll("\\[", "");
        predLog = predLog.replaceAll("\\]", ", ");

        succLog = succLog.replaceAll("\\[", "");
        succLog = succLog.replaceAll("\\]", ", ");

        Log.d("PredLog", predLog);
        Log.d("SuccLog", succLog);

        if (predLog.length() != 0) {
            String[] preArray = predLog.split(", ");
            for (String j : preArray) {
                Log.d("PREARR",j);
                String[] x = j.split(",");
                if (x[0].equals("Insert")) {
                    ContentValues values = new ContentValues();
                    values.put("key", x[1]);
                    values.put("value", x[2]);
                    sqLiteDatabase.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                } else if (x[0].equals("Delete")) {
                    String[] whereArgs = {x[1]};
                    sqLiteDatabase.delete(TABLE_NAME, COLUMN_KEY + "=?", whereArgs);
                }
            }
        }

        if (succLog.length() != 0){
            String[] sucArray = succLog.split(", ");
            for (String j : sucArray) {
                Log.d("SuccArr",j);
                String[] x = j.split(",");
                if (x[0].equals("Insert")) {
                    ContentValues values = new ContentValues();
                    values.put("key", x[1]);
                    values.put("value", x[2]);
                    sqLiteDatabase.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                } else if (x[0].equals("Delete")) {
                    String[] whereArgs = {x[1]};
                    sqLiteDatabase.delete(TABLE_NAME, COLUMN_KEY + "=?", whereArgs);
                }
            }
        }


    }

    private synchronized String insertLocally(MessageRequest request) {
        String msg = request.getMessage();
        String split[] = msg.split(",");
        ContentValues values = new ContentValues();
        values.put("key", split[0]);
        values.put("value", split[1]);
        sqLiteDatabase.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
        String savePort = request.getOriginalPort();
        if (request.getType().equals("Insert")) {
            savePort = Integer.toString(myPort);
        }
        if (insertDeleteLog.containsKey(savePort)) {
            insertDeleteLog.get(savePort).add("Insert," + msg);
        } else {
            ArrayList<String> temp = new ArrayList<String>();
            temp.add("Insert," + msg);
            insertDeleteLog.put(savePort, temp);
        }
        if (!request.getType().equals("Replica2")) {
            try {
                if (request.getType().equals("Insert")) {
                    request.setOriginalPort(Integer.toString(myPort));
                    request.setType("Replica1");
                } else {
                    request.setType("Replica2");
                }
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        successorPort * 2);
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                dataOutputStream.writeUTF(request.getJson());
                dataOutputStream.flush();
                DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
                socket.setSoTimeout(5000);
                String resp = dataInputStream.readUTF();
                return resp;
            } catch (IOException e) {
                e.printStackTrace();
                //Failed to send to Next Node
                if (request.getType().equals("Replica1")) {
                    request.setType("Replica2");
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                getNextNode(successorPort) * 2);
                        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                        dataOutputStream.writeUTF(request.getJson());
                        dataOutputStream.flush();
                        DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
                        socket.setSoTimeout(5000);
                        String resp = dataInputStream.readUTF();
                        return resp;
                    } catch (IOException f) {
                        f.printStackTrace();
                    }
                }
            }
        }
        return "Success";
    }

    private synchronized String queryLocally(String key, String originalPort) {
        if (originalPort == null) {
            String searchClause = COLUMN_KEY + " = ?";
            String[] searchQuery = {key};
            String[] colsFetch = {COLUMN_KEY, COLUMN_VALUE};
            Cursor cursor = sqLiteDatabase.query(TABLE_NAME, colsFetch, searchClause, searchQuery, null, null, null);
            cursor.moveToFirst();
            String resp = cursor.getString(1);
            cursor.close();
            return resp;
        } else {
            //* Query received.
            String resp = null;
            if (Integer.parseInt(originalPort) != successorPort) {
                MessageRequest request = new MessageRequest("Query", originalPort, "*");
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            successorPort * 2);
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeUTF(request.getJson());
                    dataOutputStream.flush();
                    DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
                    socket.setSoTimeout(5000);
                    resp = dataInputStream.readUTF();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    int newPort = getNextNode(successorPort);
                    if (Integer.parseInt(originalPort) != newPort) {
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    newPort * 2);
                            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                            dataOutputStream.writeUTF(request.getJson());
                            dataOutputStream.flush();
                            DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
                            socket.setSoTimeout(5000);
                            resp = dataInputStream.readUTF();
                            socket.close();
                        } catch (IOException f) {
                            f.printStackTrace();
                        }
                    }
                }
            }

            Cursor cursor = sqLiteDatabase.rawQuery("Select * from " + TABLE_NAME, null);
            cursor.moveToFirst();

            JSONArray keysArray = new JSONArray();
            JSONArray valuesArray = new JSONArray();
            int i = 0;
            if (resp != null) {
                try {
                    JSONObject jsonObject = new JSONObject(resp);
                    keysArray = jsonObject.getJSONArray("keys");
                    valuesArray = jsonObject.getJSONArray("values");
                    i = keysArray.length();
                } catch (JSONException e) {
                    e.printStackTrace();

                }
            }

            while (!cursor.isAfterLast()) {
                try {
                    keysArray.put(i, cursor.getString(0));
                    valuesArray.put(i, cursor.getString(1));
                    i++;
                    cursor.moveToNext();
                } catch (JSONException e) {
                    e.printStackTrace();
                }

            }
            JSONObject response = new JSONObject();
            try {
                response.put("keys", keysArray);
                response.put("values", valuesArray);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            return response.toString();
        }
    }

    private synchronized int deleteLocally(MessageRequest request) {
        if (!request.getMessage().equals("*")) {
            String[] whereArgs = {request.getMessage()};
            sqLiteDatabase.delete(TABLE_NAME, COLUMN_KEY + "=?", whereArgs);
            String savePort = request.getOriginalPort();
            if (request.getType().equals("Delete")) {
                savePort = Integer.toString(myPort);
            }
            if (insertDeleteLog.containsKey(savePort)) {
                insertDeleteLog.get(savePort).add("Delete," + request.getMessage());
            } else {
                ArrayList<String> temp = new ArrayList<String>();
                temp.add("Delete," + request.getMessage());
                insertDeleteLog.put(savePort, temp);
            }
            if (!request.getType().equals("Delete2")) {
                try {
                    if (request.getType().equals("Delete")) {
                        request.setOriginalPort(Integer.toString(myPort));
                        request.setType("Delete1");
                    } else {
                        request.setType("Delete2");
                    }
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            successorPort * 2);
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeUTF(request.getJson());
                    dataOutputStream.flush();
                    DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
                    socket.setSoTimeout(5000);
                    String resp = dataInputStream.readUTF();
                } catch (IOException e) {
                    e.printStackTrace();
                    if (request.getType().equals("Delete1")) {
                        request.setType("Delete2");
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    getNextNode(successorPort) * 2);
                            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                            dataOutputStream.writeUTF(request.getJson());
                            dataOutputStream.flush();
                            DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
                            socket.setSoTimeout(5000);
                            String resp = dataInputStream.readUTF();
                        } catch (IOException f) {
                            f.printStackTrace();
                        }
                    }
                }
            }
        } else {
            if (!request.getOriginalPort().equals(Integer.toString(successorPort))) {

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            successorPort * 2);
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeUTF(request.getJson());
                    dataOutputStream.flush();
                    DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
                    socket.setSoTimeout(5000);
                    String resp = dataInputStream.readUTF();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    int newPort = getNextNode(successorPort);
                    if (!request.getOriginalPort().equals(Integer.toString(successorPort))) {
                        try {
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    newPort * 2);
                            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                            dataOutputStream.writeUTF(request.getJson());
                            dataOutputStream.flush();
                            DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
                            socket.setSoTimeout(5000);
                            String resp = dataInputStream.readUTF();
                            socket.close();
                        } catch (IOException f) {
                            f.printStackTrace();
                        }
                    }
                }
            }
            sqLiteDatabase.delete(TABLE_NAME, null, null);
        }

        return 0;
    }

    private class ClientTask extends AsyncTask<Object, Void, String> {
        @Override
        protected String doInBackground(Object... msg) {
            MessageRequest request = (MessageRequest) msg[0];
            int remotePort = (Integer) msg[1];
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        remotePort);
                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                dataOutputStream.writeUTF(request.getJson());
                dataOutputStream.flush();
                //Log.d("MSG SENT", request.getJson());
                //Log.d("REMOTE PORT", Integer.toString(remotePort));
                DataInputStream dataInputStream = new DataInputStream((socket.getInputStream()));
                socket.setSoTimeout(5000);
                String resp = dataInputStream.readUTF();
                socket.close();
                return resp;
            } catch (SocketTimeoutException e) {
                Log.d("SocketTimeOut", "Exception for" + Integer.toString(remotePort));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                Log.d("IOEXCEPTION", "Timeout on " + Integer.toString(remotePort));
                e.printStackTrace();
            }

            return "Failure";
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, MessageRequest, Void> {

        ServerSocket serverSocket;

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            serverSocket = sockets[0];
            Log.d("ServerTask", "Server Task Started");
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    String jsonString = dataInputStream.readUTF();
                    Log.d("ServerRecd", jsonString);
                    MessageRequest request = new MessageRequest(jsonString);
                    if ((request.getType().equals("Insert")) || (request.getType().equals("Replica1"))
                            || (request.getType().equals("Replica2"))) {
                        String resp = insertLocally(request);
                        dataOutputStream.writeUTF(resp);
                        socket.close();
                    } else if (request.getType().equals("Query")) {
                        if (request.getMessage().equals("*")) {
                            dataOutputStream.writeUTF(queryLocally("*", request.getOriginalPort()));
                        } else {
                            dataOutputStream.writeUTF(queryLocally(request.getMessage(), null));
                        }
                        socket.close();
                    } else if ((request.getType().equals("Delete")) || (request.getType().equals("Delete1"))
                            || (request.getType().equals("Delete2"))) {
                        dataOutputStream.writeUTF(Integer.toString(deleteLocally(request)));
                        socket.close();
                    } else if (request.getType().equals("Recovery")) {
                        String[] fetchPorts = request.getMessage().split(",");
                        String log = "";
                        for (String f : fetchPorts) {
                            if (insertDeleteLog.containsKey(f)) {
                                log += insertDeleteLog.get(f).toString();
                            }
                        }
                        dataOutputStream.writeUTF(log);
                        socket.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                } catch (NullPointerException e){
                    e.printStackTrace();
                }
            }

            return null;
        }

        protected void onProgressUpdate(MessageRequest... requests) {
            MessageRequest request = requests[0];

            return;
        }

    }
}
