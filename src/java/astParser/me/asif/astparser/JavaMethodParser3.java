package me.asif.astparser;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseProblemException;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.BodyDeclaration;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.ConstructorDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.TypeDeclaration;
import com.github.javaparser.ast.body.InitializerDeclaration;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

public class JavaMethodParser3 {
    String javaFileContent = "public class OuterClass { "
            + "public class InnerClass { "
            + "public void innerMethod() { System.out.println(\"Inner method\"); } } "
            + "public OuterClass() { System.out.println(\"Outer constructor\"); } "
            + "public void outerMethod() { System.out.println(\"Outer method\"); } "
            + "static { System.out.println(\"Static initializer\"); } "
            + "private final String str = \"instance initializer\"; "
            + "public OuterClass(String str) { this.str = str; } }"
            + "interface MyInterface { void interfaceMethod(); }" +
            "abstract class MyAbstractClass { abstract void abstractMethod(); }"
            + "public class Pizza {\n" +
            "    private PizzaStatus status;\n" +
            "    public enum PizzaStatus {\n" +
            "        ORDERED,\n" +
            "        READY,\n" +
            "        DELIVERED;\n" +
            "    }\n" +
            "\n" +
            "    public boolean isDeliverable() {\n" +
            "        if (getStatus() == PizzaStatus.READY) {\n" +
            "            return true;\n" +
            "        }\n" +
            "        return false;\n" +
            "    }\n" +
            "    \n" +
            "    // Methods that set and get the status variable.\n" +
            "}";
    public static void main(String[] args) {
        // Example input Java file content as a string
        String javaFileContent = "public class OuterClass { "
                + "public class InnerClass { "
                + "public void innerMethod() { System.out.println(\"Inner method\"); } } "
                + "public OuterClass() { System.out.println(\"Outer constructor\"); } "
                + "public void outerMethod() { System.out.println(\"Outer method\"); } "
                + "static { System.out.println(\"Static initializer\"); } "
                + "private final String str = \"instance initializer\"; "
                + "public OuterClass(String str) { this.str = str; } }"
                + "interface MyInterface { void interfaceMethod(); }" +
                "abstract class MyAbstractClass { abstract void abstractMethod(); }"
                + "public class Pizza {\n" +
                "    private PizzaStatus status;\n" +
                "    public enum PizzaStatus {\n" +
                "        ORDERED,\n" +
                "        READY,\n" +
                "        DELIVERED;\n" +
                "    }\n" +
                "\n" +
                "    public boolean isDeliverable() {\n" +
                "        if (getStatus() == PizzaStatus.READY) {\n" +
                "            return true;\n" +
                "        }\n" +
                "        return false;\n" +
                "    }\n" +
                "    \n" +
                "    // Methods that set and get the status variable.\n" +
                "}";

        // Process the Java file content and print the output
        String output = processJavaFileContent(javaFileContent);
        System.out.println(output);
    }

    public static String processJavaFileContent(String javaFileContent) {
        try {
            // Create a JavaParser instance
            JavaParser javaParser = new JavaParser();

            // Parse the Java file content
            CompilationUnit compilationUnit = javaParser.parse(javaFileContent).getResult().get();

            // Collect method and constructor information
            JSONArray membersJsonArray = new JSONArray();
            processTypes(compilationUnit, membersJsonArray);

            // Return the JSON array as a string
            return membersJsonArray.toString(2);
        } catch (ParseProblemException e) {
            // Handle parsing problems
            e.printStackTrace();
            return "";
        } catch (Exception e) {
            // Handle any other exceptions
            e.printStackTrace();
            return "";
        }
    }

    private static void processTypes(CompilationUnit cu, JSONArray membersJsonArray) {
        for (TypeDeclaration<?> type : cu.getTypes()) {
            processType(type, membersJsonArray);
        }
    }

    private static void processType(TypeDeclaration<?> type, JSONArray membersJsonArray) {
        if (type.isClassOrInterfaceDeclaration()) {
            ClassOrInterfaceDeclaration classOrInterface = type.asClassOrInterfaceDeclaration();
            String className = classOrInterface.getNameAsString();

            // Process methods
            List<MethodDeclaration> methods = classOrInterface.getMethods();
            for (MethodDeclaration method : methods) {
                JSONObject methodJson = new JSONObject();
                methodJson.put("class_name", className);
                methodJson.put("member_type", "method");
                methodJson.put("member_name", method.getNameAsString());
                methodJson.put("member_body", method.toString());
                membersJsonArray.put(methodJson);
            }

            // Process constructors
            List<ConstructorDeclaration> constructors = classOrInterface.getConstructors();
            for (ConstructorDeclaration constructor : constructors) {
                JSONObject constructorJson = new JSONObject();
                constructorJson.put("class_name", className);
                constructorJson.put("member_type", "constructor");
                constructorJson.put("member_name", constructor.getNameAsString());
                constructorJson.put("member_body", constructor.toString());
                membersJsonArray.put(constructorJson);
            }

            // Process static initializers
//            List<InitializerDeclaration> staticInitializers = classOrInterface.getStaticInitializers();
//            for (InitializerDeclaration initializer : staticInitializers) {
//                JSONObject initializerJson = new JSONObject();
//                initializerJson.put("class_name", className);
//                initializerJson.put("member_type", "static_initializer");
//                initializerJson.put("member_name", "static_initializer");
//                initializerJson.put("member_body", initializer.getBody().toString());
//                membersJsonArray.put(initializerJson);
//            }

            // Process instance initializers
            List<BodyDeclaration<?>> innerMembers = classOrInterface.getMembers();
            for (BodyDeclaration<?> innerMember : innerMembers) {
                if (innerMember.isInitializerDeclaration()) {
                    InitializerDeclaration initializer = innerMember.asInitializerDeclaration();
                    JSONObject initializerJson = new JSONObject();
                    initializerJson.put("class_name", className);
                    initializerJson.put("member_type", "instance_initializer");
                    initializerJson.put("member_name", "instance_initializer");
                    initializerJson.put("member_body", initializer.getBody().toString());
                    membersJsonArray.put(initializerJson);
                }
            }

            // Process inner classes and interfaces recursively
            for (BodyDeclaration<?> innerMember : classOrInterface.getMembers()) {
                if (innerMember.isClassOrInterfaceDeclaration()) {
                    processType(innerMember.asClassOrInterfaceDeclaration(), membersJsonArray);
                }
            }
        }
    }
}
