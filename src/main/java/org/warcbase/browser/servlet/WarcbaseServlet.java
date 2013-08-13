package org.warcbase.browser.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.warcbase.ResponseRecord;
import org.warcbase.TextDocument2;
import org.warcbase.Util;
import org.warcbase.warcRecordParser;

public class WarcbaseServlet extends HttpServlet {
  private static final long serialVersionUID = 847405540723915805L;

  private final Configuration hbaseConfig;
  private final String name;

  public WarcbaseServlet(String name) {
    this.hbaseConfig = HBaseConfiguration.create();
    this.name = name;
  }

  private void writeResponse(HttpServletResponse resp, byte[] data, String query, String d)
      throws IOException {
    String content = new String(data, "UTF8");

    if (!warcRecordParser.getType(content).startsWith("text")) {
      resp.setHeader("Content-Type", ResponseRecord.getType(content));
      resp.setContentLength(ResponseRecord.getBodyByte(data).length);
      resp.getOutputStream().write(ResponseRecord.getBodyByte(data));
    } else {
      System.setProperty("file.encoding", "UTF8");
      resp.setHeader("Content-Type", ResponseRecord.getType(content));
      resp.setCharacterEncoding("UTF-8");
      PrintWriter out = resp.getWriter();
      TextDocument2 t2 = new TextDocument2(null, null, null);
      String bodyContent = new String(ResponseRecord.getBodyByte(data), "UTF8");
      bodyContent = t2.fixURLs(bodyContent, query, d);
      out.println(bodyContent);
    }
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String query = req.getParameter("query");
    String d = req.getParameter("date");

    String q = Util.reverseHostname(query);
    HTable table = new HTable(hbaseConfig, name);
    Get get = new Get(Bytes.toBytes(q));
    Result rs = table.get(get);
    byte[] data = null;

    for (int i = 1; i < rs.raw().length; i++) {
      System.out.println(rs.raw()[i].getValue().length + " " + rs.raw()[i - 1].getValue().length);
      if (Arrays.equals(ResponseRecord.getBodyByte(rs.raw()[i].getValue()),
          ResponseRecord.getBodyByte(rs.raw()[i - 1].getValue())))
        System.out.println("++++++++++++=================++++++++++++++");
    }

    if (rs.raw().length == 0) {
      PrintWriter out = resp.getWriter();
      out.println("Not Found.");
      table.close();
      return;
    }
    if (d != null) {
      for (int i = 0; i < rs.raw().length; i++) {
        String date = new String(rs.raw()[i].getQualifier());
        if (date.equals(d)) {
          data = rs.raw()[i].getValue();
          writeResponse(resp, data, query, d);
          table.close();
          return;
        }
      }
      ArrayList<String> dates = new ArrayList<String>(10);
      for (int i = 0; i < rs.raw().length; i++)
        dates.add(new String(rs.raw()[i].getQualifier()));
      Collections.sort(dates);
      for (int i = 1; i < dates.size(); i++)
        if (dates.get(i).compareTo(d) > 0) {// d < i
          data = rs.raw()[i - 1].getValue();
          writeResponse(resp, data, query, d);
          table.close();
          return;
        }
      int i = dates.size();
      data = rs.raw()[i - 1].getValue();
      writeResponse(resp, data, query, d);
      table.close();
      return;
    }

    PrintWriter out = resp.getWriter();
    out.println("<html>");
    out.println("<body>");
    for (int i = 0; i < rs.raw().length; i++) {
      String date = new String(rs.raw()[i].getQualifier());
      out.println("<br/> <a href='http://" + req.getServerName() + ":" + req.getServerPort()
          + req.getRequestURI() + "?query=" + req.getParameter("query") + "&date=" + date + "'>"
          + date + "</a>");
    }
    out.println("</body>");
    out.println("</html>");
    table.close();
  }

  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
      IOException {
    String field = req.getParameter("field");
    PrintWriter out = resp.getWriter();

    out.println("<html>");
    out.println("<body>");
    out.println("You entered \"" + field + "\" into the text box.");
    out.println("</body>");
    out.println("</html>");
  }
}