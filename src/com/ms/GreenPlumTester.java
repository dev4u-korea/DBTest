package com.ms;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class GreenPlumTester {

    private Connection _con;

    public void Test() {

        try {
            Init();
        } catch (Exception e){
            e.printStackTrace();
        }

        try {
            TestMain();
        } catch (Exception e){
            e.printStackTrace();
        }

        try {
            Close();
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    private void Init() throws Exception  {
        try {
            Class.forName("org.postgresql.Driver");
            _con = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:15432/mobdw?rewriteBatchedInserts=true", "dwdba","kms4693");

            if (_con != null) {
                System.out.println("DB Connect Succ!");
            } else {
                System.out.println("DB Connect Fail!");
            }

            _con.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private void TestMain() throws Exception {

        PreparedStatement pstmt;

        pstmt = _con.prepareStatement("insert into morms.tt_trade values(?,?,?,?)");

        for (int i=0; i <= 300000; i++) {

            pstmt.setString(1, String.valueOf(i));
            pstmt.setString(2, String.valueOf(i*10));
            pstmt.setString(3, String.valueOf(i*20));
            pstmt.setString(4, String.valueOf(i*30));

            pstmt.addBatch();

            if (i%1000 == 0){
                pstmt.executeBatch();
                _con.commit();
            }
        }

        _con.commit();
    }

    private void Close() throws SQLException {
        if (_con != null) {
            _con.close();
        }
    }
}
