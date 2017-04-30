package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {
    private ContentResolver contentResolver;
    private Uri uri;
    private TextView tv;

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
        contentResolver = getContentResolver();
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledynamo.provider");
        uriBuilder.scheme("content");
        uri = uriBuilder.build();
    
		tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        findViewById(R.id.button_test).setOnClickListener(
				new OnTestClickListener(tv, getContentResolver()));
        findViewById(R.id.button_ldump).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Cursor resultCursor = contentResolver.query(uri, null,
                        "@", null, null);
                displayCursorOnTextView(resultCursor);
            }
        });
	}

    public void displayCursorOnTextView(Cursor cursor) {
        Log.d("Cursor Size:", Integer.toString(cursor.getCount()));
        if (cursor.moveToFirst()) {
            while (!cursor.isAfterLast()) {
                tv.append(cursor.getString(0) + ":" + cursor.getString(1) + "\n");
                cursor.moveToNext();
            }
        } else {
            tv.append("Empty Result returned!\n");
        }
        cursor.close();
    }

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
