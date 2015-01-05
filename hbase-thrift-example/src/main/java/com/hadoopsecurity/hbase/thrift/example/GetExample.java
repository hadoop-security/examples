package com.hadoopsecurity.hbase.thrift.example;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class GetExample {

  public static void main(String[] args) throws SaslException, TTransportException, UnsupportedEncodingException, TException {
    // java -jar hbase-thrift-example-*.jar thrift.example.com 9090 thrift table rowId
    final String host = args[0];
    final int port = Integer.parseInt(args[1]);
    final String serviceShortName = args[2];
    final String table = args[3];
    final String rowId = args[4];

    final Map<String, String> saslProperties = new HashMap<String, String>();
    saslProperties.put(Sasl.QOP, "auth");

    LoginContext loginContext = null;
    try {
      loginContext = new LoginContext(GetExample.class.getName(),
          new CallbackHandler() {

            @Override
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            }
          });

      loginContext.login();
    } catch (LoginException ex) {
      System.err.println("Authentication failed: " + ex.getMessage());
      System.exit(-1);
    }

    Subject subject = loginContext.getSubject();

    Subject.doAs(subject, new PrivilegedAction<String>() {

      @Override
      public String run() {
        try {
          TTransport socketTransport = new TSocket(host, port, 30000);
//          TTransport transport = socketTransport;
          TTransport transport = new TSaslClientTransport("GSSAPI", null,
              serviceShortName, host, saslProperties, null, socketTransport);
          System.out.println("Open transport");
          transport.open();

          System.out.println("Create client");
          TProtocol protocol = new TBinaryProtocol(transport);
          Hbase.Client client = new Hbase.Client(protocol);

          System.out.println("Get row");
          List<TRowResult> rows = client.getRow(
              ByteBuffer.wrap(table.getBytes("UTF-8")),
              ByteBuffer.wrap(rowId.getBytes("UTF-8")), null);

          TRowResult row = rows.iterator().next();
          System.out.println(row.toString());

          transport.close();

          return row.toString();
        } catch (Exception ex) {
          System.err.println("Get failed: " + ex.getMessage());
          throw new RuntimeException(ex);
        }
      }
    });

  }

}
