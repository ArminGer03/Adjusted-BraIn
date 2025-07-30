package me.asif.astparser;

import py4j.GatewayServer;

public class HelloWorldEntryPoint {
    private HelloWorld helloWorld;

    public HelloWorldEntryPoint() {
        helloWorld = new HelloWorld();
    }

    public HelloWorld getHelloWorld() {
        return helloWorld;
    }

    public static void main(String[] args) {
        HelloWorldEntryPoint entryPoint = new HelloWorldEntryPoint();
        GatewayServer server = new GatewayServer(entryPoint);
        server.start();
        System.out.println("Gateway Server Started");
    }
}
