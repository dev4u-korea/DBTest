package com.ms;

public class Main {

    public static void main(String[] args) {

        long beforeTime = System.currentTimeMillis(); //코드 실행 전에 시간 받아오기
	// write your code here

  //      PostgresTester tester = new PostgresTester();
  //      GreenPlumTester tester = new GreenPlumTester();
        MariaTester tester = new MariaTester();
  //      YugabyteTester tester = new YugabyteTester();
  //      YugabyteNewTester tester = new YugabyteNewTester();
  //      ClickHouseTester tester = new ClickHouseTester();

        tester.Test();

        long afterTime = System.currentTimeMillis(); // 코드 실행 후에 시간 받아오기
        long secDiffTime = (afterTime - beforeTime)/1000; //두 시간에 차 계산
        System.out.println("시간차이(s) : "+secDiffTime);


    }
}
