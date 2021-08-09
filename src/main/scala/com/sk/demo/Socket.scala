package com.sk.demo

import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.ServerSocket

object Socket {
  def main(args: Array[String]): Unit = {
    val serverSocket = new ServerSocket(9999);

    val socket = serverSocket.accept();

    while(true) {
      val bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

        bw.write("hello scala ")
        Thread.sleep(200);
        bw.write("hello word ");
        Thread.sleep(200);
        bw.write("hello spark ");
        Thread.sleep(200);
        bw.write("how are you ");
        Thread.sleep(200);
        System.out.println("aaa bbb ");



      //bufferedwrite需要调用newLine方法，
      //flush方法必须添加
      bw.newLine();
      bw.flush();
    }
  }
}
