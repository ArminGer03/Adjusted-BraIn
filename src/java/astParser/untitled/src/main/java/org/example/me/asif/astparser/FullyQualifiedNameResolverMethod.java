package org.example.me.asif.astparser;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.body.ConstructorDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import com.github.javaparser.resolution.declarations.ResolvedMethodDeclaration;
import com.github.javaparser.resolution.declarations.ResolvedReferenceTypeDeclaration;
import com.github.javaparser.symbolsolver.JavaSymbolSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.ReflectionTypeSolver;

import java.io.FileInputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class FullyQualifiedNameResolverMethod {
    public static void main(String[] args) throws Exception {
        FullyQualifiedNameResolverMethod fullyQualifiedNameResolver = new FullyQualifiedNameResolverMethod();
        ArrayList<String> methods = new ArrayList<>();
        methods.add("resolveVariable");
        methods.add("setExchange");


        HashSet<String> fullyQualifiedNames = fullyQualifiedNameResolver.resolveFullyQualifiedNames("D:\\Research\\Data\\Intelligent_Feedback\\Sources\\Apache\\CAMEL\\camel-1.1.0\\19a1fc2f7c0fd2c215c777eb58421904.java", false, methods);
        for (String fqn : fullyQualifiedNames) {
            System.out.println(fqn);
        }
    }

    HashSet<String> methodsToCheck = new HashSet<>();
    HashSet<String> methodCallsFQN = new HashSet<>();
    List<String> imports2 = new ArrayList();

    public HashSet<String> resolveFullyQualifiedNames(String filePathOrCode, boolean isCode, ArrayList<String> methods) throws Exception {
        // Create a CombinedTypeSolver
        CombinedTypeSolver combinedTypeSolver = new CombinedTypeSolver();
        combinedTypeSolver.add(new ReflectionTypeSolver());

        // Configure JavaParser to use the combined type solver
        JavaSymbolSolver symbolSolver = new JavaSymbolSolver(combinedTypeSolver);
        StaticJavaParser.getConfiguration().setSymbolResolver(symbolSolver);

        // Parse the Java file
        CompilationUnit cu;
        if (!isCode) {
            FileInputStream in = new FileInputStream(filePathOrCode);
            cu = StaticJavaParser.parse(in);
        } else {
            cu = StaticJavaParser.parse(filePathOrCode);
        }

        methodCallsFQN = new HashSet<>();
        methodsToCheck = new HashSet<>();
        methodsToCheck.addAll(methods);
        imports2 = new ArrayList();

        List<ImportDeclaration> imports = cu.getImports();
        for (ImportDeclaration importDeclaration : imports) {
            String statement = importDeclaration.getNameAsString();
            if (statement.endsWith("\r\n"))
                statement = statement.substring(0, statement.length() - 2);

            if (statement.startsWith("java"))
                continue;
            imports2.add(statement);
        }

        // Visit all method calls in the Java file
        MethodVisitor mv = new MethodVisitor();
//        MethodCallVisitor mcv = new MethodCallVisitor();
        mv.visit(cu, null);


        return methodCallsFQN;
    }

    public String resolveFullyQualifiedNamesJSON(String filePathOrCode, boolean isCode, String methodstring) throws Exception {
        ArrayList<String> methods = new ArrayList<>();
//        split the methodstring by comma
        String[] methodArray = methodstring.split(", ");
        for (String method : methodArray) {
            if (method.contains("!")){
                method = method.substring(0, method.indexOf("!"));
            }
            methods.add(method);
//            System.out.println(method);
        }

        CombinedTypeSolver combinedTypeSolver = new CombinedTypeSolver();
        combinedTypeSolver.add(new ReflectionTypeSolver());

        // Configure JavaParser to use the combined type solver
        JavaSymbolSolver symbolSolver = new JavaSymbolSolver(combinedTypeSolver);
        StaticJavaParser.getConfiguration().setSymbolResolver(symbolSolver);

        // Parse the Java file
        CompilationUnit cu;
        if (!isCode) {
            FileInputStream in = new FileInputStream(filePathOrCode);
            cu = StaticJavaParser.parse(in);
        } else {
            cu = StaticJavaParser.parse(filePathOrCode);
        }

        methodCallsFQN = new HashSet<>();
        methodsToCheck = new HashSet<>();
        methodsToCheck.addAll(methods);

        List<ImportDeclaration> imports = cu.getImports();
        for (ImportDeclaration importDeclaration : imports) {
            String statement = importDeclaration.getNameAsString().trim();
            if (statement.startsWith("java")){
//                System.out.println(statement);
                continue;
            }
            if (statement.endsWith("\r\n"))
                statement = statement.substring(0, statement.length() - 2);


            imports2.add(statement);
        }

        // Visit all method calls in the Java file
        MethodVisitor mv = new MethodVisitor();
//        MethodCallVisitor mcv = new MethodCallVisitor();
        mv.visit(cu, null);

//        convert this methodCallsFQN to a json array using org.json.JSONArray
        org.json.JSONArray jsonArray = new org.json.JSONArray();
        for (String fqn : methodCallsFQN) {
            jsonArray.put(fqn);
        }

        methodCallsFQN.clear();
        methodsToCheck.clear();

        return jsonArray.toString();
//        return methodCallsFQN;
    }

//    private class ConstructorVisitor extends VoidVisitorAdapter<Void> {
//        @Override
//        public void visit(ConstructorDeclaration md, Void arg) {
//            super.visit(md, arg);
//
//            if (!methodsToCheck.contains(md.getNameAsString()))
//                return;
//
//            // Visit and print all method calls inside this method
//            md.accept(new MethodCallVisitor(), null);
//            md.accept(new MethodCallVisitor2(), null);
//        }
//    }

    private class MethodVisitor extends VoidVisitorAdapter<Void> {

        @Override
        public void visit(MethodDeclaration md, Void arg) {
            super.visit(md, arg);


//            System.out.println("Method: " + md.getName());

            if (!methodsToCheck.contains(md.getNameAsString()))
                return;

            // Visit and print all method calls inside this method
            md.accept(new MethodCallVisitor(), null);
            md.accept(new MethodCallVisitor2(), null);
        }
    }

    private class MethodCallVisitor extends VoidVisitorAdapter<Void> {
        @Override
        public void visit(MethodCallExpr methodCall, Void arg) {
            super.visit(methodCall, arg);
            try {
                ResolvedMethodDeclaration resolvedMethod = methodCall.resolve();
//                get the name of the current method


//                System.out.println("Method Call: " + methodCall.getName());

                // Get the class name where the method is declared
                String className = resolvedMethod.getClassName();
//                System.out.println("Class Name: " + className);

                // Get the FQN of the class
                ResolvedReferenceTypeDeclaration declaringType = resolvedMethod.declaringType();
                String classFQN = declaringType.getQualifiedName();
//                System.out.println("Fully Qualified Class Name: " + classFQN);

                if(classFQN.startsWith("java"))
                    return;

                // Add the FQN of the method call to the set
                methodCallsFQN.add(classFQN + ".java");


            } catch (Exception e) {

            }


        }
    }

    private class MethodCallVisitor2 extends VoidVisitorAdapter<Void> {


        @Override
        public void visit(MethodCallExpr methodCall, Void arg) {
            super.visit(methodCall, arg);
            AtomicReference<String> fqn = new AtomicReference<>();

            // Get the object on which the method is called (e.g., 'exchange' in 'exchange.getIn()')
            methodCall.getScope().ifPresent(scope -> {
                String variableName = scope.toString();

                // Visit all variable declarations to find where the object was declared
                methodCall.findAncestor(CompilationUnit.class).ifPresent(cu -> {
                    Optional<VariableDeclarator> declaration = cu.findAll(VariableDeclarator.class)
                            .stream()
                            .filter(v -> v.getNameAsString().equals(variableName))
                            .findFirst();

                    // If found, print the declaration (e.g., 'Exchange exchange')
                    declaration.ifPresent(variableDeclarator ->
//                            System.out.println("Declaration: " + variableDeclarator.getType().toString() + " " + variableDeclarator.getName() + ";")
                                    fqn.set(variableDeclarator.getType().toString())

                    );
                });
            });

            String fqnString = fqn.get();
            if (fqnString == null) {
                return;
            }
            for (String importDeclaration : imports2) {

                if (importDeclaration.endsWith(fqnString)) {
//                    System.out.println("Import Declaration: " + importDeclaration);
                    if (importDeclaration.startsWith("java"))
                        break;

                    methodCallsFQN.add(importDeclaration + ".java");
                    break;
                }
            }
        }
    }
}
