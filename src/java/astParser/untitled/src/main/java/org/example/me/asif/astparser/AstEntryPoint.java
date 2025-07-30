package org.example.me.asif.astparser;

import py4j.GatewayServer;

public class AstEntryPoint {
    JavaMethodParser3 javaMethodParser = new JavaMethodParser3();
    public AstEntryPoint() {
        // Create a new instance of the AstParser class
//        AstParser astParser = new AstParser();
//        // Start the parser
//        astParser.start();
    }

    public String helloWorld() {
        return "Hello world!";
    }

//    public JavaMethodParser getJavaMethodParser() {
//        return javaMethodParser;
//    }
    public JavaMethodParser3 getJavaMethodParser() {
        return javaMethodParser;
    }

    public FullyQualifiedNameResolver getFullyQualifiedNameResolver() {
        return new FullyQualifiedNameResolver();
    }

    public FullyQualifiedNameResolverMethod getFullyQualifiedNameResolverMethod() {
        return new FullyQualifiedNameResolverMethod();
    }

    public static void main(String[] args) {
        AstEntryPoint astEntryPoint = new AstEntryPoint();
        GatewayServer gatewayServer = new GatewayServer(astEntryPoint);

        gatewayServer.start();
        System.out.println("Gateway Server Started");
    }
}
