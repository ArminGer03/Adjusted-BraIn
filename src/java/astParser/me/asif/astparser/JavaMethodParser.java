package me.asif.astparser;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.ConstructorDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import com.github.javaparser.ast.body.EnumDeclaration;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

public class JavaMethodParser {

    public static void main(String[] args) {
        // Example input Java file content as a string
        String javaFileContent = "public class Test { public Test() { System.out.println(\"Constructor\"); } public void method1() { System.out.println(\"Hello\"); } public void method2() { System.out.println(\"Hello\"); } }"
                + "class Test2 { public Test2() { System.out.println(\"Constructor\"); } public void method1() { System.out.println(\"Hello\"); } public void method2() { System.out.println(\"Hello\"); } }"
                + "interface MyInterface { void interfaceMethod(); }"
                + "abstract class MyAbstractClass { abstract void abstractMethod(); }";
//                + "public enum MyEnum { CONSTANT1, CONSTANT2; public void print() { System.out.println(\\\"Enum Method\\\"); } ";

        // Create a JavaParser instance
        JavaParser javaParser = new JavaParser();

        // Parse the Java file content
        CompilationUnit compilationUnit = javaParser.parse(javaFileContent).getResult().get();

        // Collect method and constructor information
        JSONArray membersJsonArray = new JSONArray();
        new MemberVisitor().visit(compilationUnit, membersJsonArray);

        // Output the JSON array
        System.out.println(membersJsonArray.toString(2));
    }

    // method to return the JSON array of method and constructor information
    public JSONArray getMethodAndConstructorInfo(String javaFileContent) {
        // Create a JavaParser instance
        JavaParser javaParser = new JavaParser();

        // Parse the Java file content
        CompilationUnit compilationUnit = javaParser.parse(javaFileContent).getResult().get();

        // Collect method and constructor information
        JSONArray membersJsonArray = new JSONArray();
        new MemberVisitor().visit(compilationUnit, membersJsonArray);

        return membersJsonArray;
    }

    private static class MemberVisitor extends VoidVisitorAdapter<JSONArray> {
        @Override
        public void visit(EnumDeclaration n, JSONArray arg) {
            String enumName = n.getNameAsString();

            // Process enum methods
            List<MethodDeclaration> methods = n.getMethods();
            for (MethodDeclaration method : methods) {
                JSONObject methodJson = new JSONObject();
                methodJson.put("enum_name", enumName);
                methodJson.put("member_type", "method");
                methodJson.put("member_name", method.getNameAsString());
                methodJson.put("member_body", method.toString());
                arg.put(methodJson);
            }

            super.visit(n, arg);
        }

        @Override
        public void visit(ClassOrInterfaceDeclaration n, JSONArray arg) {

            if (!n.isEnumDeclaration()) {
                // Only process non-enum classes
                String className = n.getNameAsString();

                // Process methods
                List<MethodDeclaration> methods = n.getMethods();
                for (MethodDeclaration method : methods) {
                    JSONObject methodJson = new JSONObject();
                    methodJson.put("class_name", className);
                    methodJson.put("member_type", "method");
                    methodJson.put("member_name", method.getNameAsString());
                    methodJson.put("member_body", method.toString());
                    arg.put(methodJson);
                }

                // Process constructors
                List<ConstructorDeclaration> constructors = n.getConstructors();
                for (ConstructorDeclaration constructor : constructors) {
                    JSONObject constructorJson = new JSONObject();
                    constructorJson.put("class_name", className);
                    constructorJson.put("member_type", "constructor");
                    constructorJson.put("member_name", constructor.getNameAsString());
                    constructorJson.put("member_body", constructor.toString());
                    arg.put(constructorJson);
                }
            }
            super.visit(n, arg);
        }
    }
}


