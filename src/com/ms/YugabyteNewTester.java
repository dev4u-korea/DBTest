package com.ms;

import java.sql.*;

public class YugabyteNewTester {

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
            _con = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5433/yb_demo?rewriteBatchedInserts=true", "moonsun","moonsun1234");

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




        StringBuilder sbQry = new StringBuilder();

        boolean bCommit = true;
        Statement stmt = _con.createStatement();
        for (int i=0; i < 300000; i++) {

            if (bCommit) {
                sbQry = new StringBuilder();
                sbQry.append("insert into public.tt_trade values");
                bCommit = false;
            }

            sbQry.append(String.format("(%s,%s,%s,%s)",String.valueOf(i),String.valueOf(i*10),String.valueOf(i*20),String.valueOf(i*30)));

            if (i%1000 == 0){

                stmt.execute(sbQry.toString());
                _con.commit();
                bCommit = true;

            } else {
                sbQry.append(",");
            }
        }
        stmt.close();
        _con.commit();


    }

    private void Close() throws SQLException {
        if (_con != null) {
            _con.close();
        }
    }
}
