package me.asif.astparser;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import com.github.javaparser.resolution.declarations.ResolvedMethodDeclaration;
import com.github.javaparser.resolution.declarations.ResolvedReferenceTypeDeclaration;
import com.github.javaparser.symbolsolver.JavaSymbolSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.ReflectionTypeSolver;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class FullyQualifiedNameResolver {
    public static void main(String[] args) throws Exception {
        FullyQualifiedNameResolver fullyQualifiedNameResolver = new FullyQualifiedNameResolver();

        HashSet<String> fullyQualifiedNames = fullyQualifiedNameResolver.resolveFullyQualifiedNames("D:\\Research\\Data\\Intelligent_Feedback\\Sources\\Apache\\CAMEL\\camel-1.1.0\\19a1fc2f7c0fd2c215c777eb58421904.java", false);
        for (String fqn : fullyQualifiedNames) {
            System.out.println(fqn);
        }
    }

    public HashSet<String> resolveFullyQualifiedNames(String filePathOrCode, boolean isCode) throws Exception {
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
        // Visit all method calls in the Java file
        MethodCallVisitor mcv = new MethodCallVisitor();
        mcv.visit(cu, null);
        HashSet<String> methodCallsFQN = mcv.methodCallsFQN;

        MethodCallVisitor2 mcv2 = new MethodCallVisitor2();
        List<ImportDeclaration> imports = cu.getImports();
        List<String> imports2 = new ArrayList();

        for (ImportDeclaration importDeclaration : imports) {
            String statement = importDeclaration.getNameAsString();
            if(statement.endsWith("\r\n"))
                statement = statement.substring(0, statement.length() - 2);
            imports2.add(statement);
        }

        mcv2.imports = imports2;
        HashSet<String> methodCallsFQN2 = mcv2.methodCallsFQN;
        mcv2.visit(cu, null);

//        add methodCallsFQN2 to methodCallsFQN
        methodCallsFQN.addAll(methodCallsFQN2);

        return methodCallsFQN;
    }

    public String resolveFullyQualifiedNamesJSON(String filePathOrCode, boolean isCode) throws Exception {

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
        // Visit all method calls in the Java file
        MethodCallVisitor mcv = new MethodCallVisitor();
        mcv.visit(cu, null);
        HashSet<String> methodCallsFQN = mcv.methodCallsFQN;

        MethodCallVisitor2 mcv2 = new MethodCallVisitor2();
        List<ImportDeclaration> imports = cu.getImports();
        List<String> imports2 = new ArrayList();

        for (ImportDeclaration importDeclaration : imports) {
            String statement = importDeclaration.getNameAsString();
            if(statement.endsWith("\r\n"))
                statement = statement.substring(0, statement.length() - 2);
            imports2.add(statement);
        }

        mcv2.imports = imports2;
        HashSet<String> methodCallsFQN2 = mcv2.methodCallsFQN;
        mcv2.visit(cu, null);

//        add methodCallsFQN2 to methodCallsFQN
        methodCallsFQN.addAll(methodCallsFQN2);

//        remove fqns starts with java
//        HashSet<String> methodCallsFQN3 = new HashSet<>();
//        for (String fqn : methodCallsFQN) {
//            if (!fqn.startsWith("java")) {
//                methodCallsFQN3.add(fqn);
//            }
//        }
//        methodCallsFQN = methodCallsFQN3;

//        convert this methodCallsFQN to a json array using org.json.JSONArray
        org.json.JSONArray jsonArray = new org.json.JSONArray();
        for (String fqn : methodCallsFQN) {
            jsonArray.put(fqn);
        }

        return jsonArray.toString();
//        return methodCallsFQN;
    }

    private class MethodCallVisitor extends VoidVisitorAdapter<Void> {
        HashSet<String> methodCallsFQN = new HashSet<>();
        @Override
        public void visit(MethodCallExpr methodCall, Void arg) {
            super.visit(methodCall, arg);
            try {
                ResolvedMethodDeclaration resolvedMethod = methodCall.resolve();
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
        HashSet<String> methodCallsFQN = new HashSet<>();
        List<String> imports;

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
            for (String importDeclaration : imports) {

                if (importDeclaration.endsWith(fqnString)) {
//                    System.out.println("Import Declaration: " + importDeclaration);
                    if (importDeclaration.startsWith("java"))
                            break;

                    methodCallsFQN.add(importDeclaration+".java");
                    break;
                }
            }
        }
    }
}
