package me.asif.astparser;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.BodyDeclaration;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.ConstructorDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.TypeDeclaration;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.List;

public class JavaMethodParser2 {

    public static void main(String[] args) {
        // Example input Java file content as a string
        String javaFileContent = "public class OuterClass { "
                + "public class InnerClass { "
                + "public void innerMethod() { System.out.println(\"Inner method\"); } } "
                + "public OuterClass() { System.out.println(\"Outer constructor\"); } "
                + "public void outerMethod() { System.out.println(\"Outer method\"); } }"
                + "interface MyInterface { void interfaceMethod(); }"
                +"public class Pizza {\n" +
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

    private static class MemberVisitor extends VoidVisitorAdapter<JSONArray> {
        @Override
        public void visit(CompilationUnit cu, JSONArray arg) {
            for (TypeDeclaration<?> type : cu.getTypes()) {
                // Process the top-level type (outer class/interface)
                handleClassOrInterface(type, arg);
            }
            super.visit(cu, arg);
        }

        private void handleClassOrInterface(TypeDeclaration<?> type, JSONArray arg) {
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
                    arg.put(methodJson);
                }

                // Process constructors
                List<ConstructorDeclaration> constructors = classOrInterface.getConstructors();
                for (ConstructorDeclaration constructor : constructors) {
                    JSONObject constructorJson = new JSONObject();
                    constructorJson.put("class_name", className);
                    constructorJson.put("member_type", "constructor");
                    constructorJson.put("member_name", constructor.getNameAsString());
                    constructorJson.put("member_body", constructor.toString());
                    arg.put(constructorJson);
                }

                // Process inner classes recursively
                List<BodyDeclaration<?>> innerMembers = classOrInterface.getMembers();
                for (BodyDeclaration<?> innerMember : innerMembers) {
                    if (innerMember.isClassOrInterfaceDeclaration()) {
                        handleClassOrInterface(innerMember.asClassOrInterfaceDeclaration(), arg);
                    }
                }
            }
        }
    }
}

