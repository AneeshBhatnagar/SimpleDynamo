package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

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

public class SimpleDynamoProvider extends ContentProvider {
    private final int SERVER_PORT = 10000;
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
    private ServerSocket socket;


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

        try {
            socket = new ServerSocket(SERVER_PORT);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, socket);

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
                if (resp.equals("OK")) {
                    socket.close();
                } else {
                    socket.close();
                    return resp;
                }
            } catch (SocketTimeoutException e) {
                Log.d("SocketTimeOut", "Exception for" + Integer.toString(remotePort));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                Log.d("IOEXCEPTION", "Timeout on " + Integer.toString(remotePort));
                e.printStackTrace();
                return null;
            }

            return null;
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
                    //Request request = new Request(jsonString);
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
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
