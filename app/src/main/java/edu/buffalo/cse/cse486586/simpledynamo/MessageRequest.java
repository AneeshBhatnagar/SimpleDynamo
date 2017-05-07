package edu.buffalo.cse.cse486586.simpledynamo;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by aneesh on 4/30/17.
 */

public class MessageRequest {
    private static String[] jsonFields = {"type", "originalPort", "message"};
    private String type;
    private String originalPort;
    private String message;

    public MessageRequest() {
        this.type = this.originalPort = this.message = null;
    }

    public MessageRequest(String type, String originalPort, String message) {
        this.type = type;
        this.originalPort = originalPort;
        this.message = message;
    }

    public MessageRequest(String type, String originalPort) {
        this.type = type;
        this.originalPort = originalPort;
        this.message = "";
    }

    public MessageRequest(String json) {
        try {
            JSONObject jsonObject = new JSONObject(json);
            this.type = jsonObject.getString(jsonFields[0]);
            this.originalPort = jsonObject.getString(jsonFields[1]);
            this.message = jsonObject.getString(jsonFields[2]);
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getOriginalPort() {
        return originalPort;
    }

    public String getJson() {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put(jsonFields[0], type);
            jsonObject.put(jsonFields[1], originalPort);
            jsonObject.put(jsonFields[2], message);
        } catch (JSONException e) {
            e.printStackTrace();
            return null;
        }
        return jsonObject.toString();
    }

    public void setOriginalPort(String originalPort) {
        this.originalPort = originalPort;
    }
}
