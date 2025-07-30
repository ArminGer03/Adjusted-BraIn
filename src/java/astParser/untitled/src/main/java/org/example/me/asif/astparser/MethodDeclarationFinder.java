package org.example.me.asif.astparser;

import com.github.javaparser.JavaParser;
import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.VariableDeclarationExpr;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Optional;

public class MethodDeclarationFinder {

    public static void main(String[] args) throws FileNotFoundException {
        // Provide the path to your Java file
        FileInputStream in = new FileInputStream("D:\\Research\\Data\\Intelligent_Feedback\\Sources\\Apache\\CAMEL\\camel-1.1.0\\19a1fc2f7c0fd2c215c777eb58421904.java");
        CompilationUnit cu = StaticJavaParser.parse(in);

        // Visit the methods and method calls in the file
        cu.accept(new MethodCallVisitor(), null);
    }

    private static class MethodCallVisitor extends VoidVisitorAdapter<Void> {
        HashSet<String> methodCallsFQN = new HashSet<>();
        @Override
        public void visit(MethodCallExpr methodCall, Void arg) {
            super.visit(methodCall, arg);

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
                            System.out.println("Declaration: " + variableDeclarator.getType() + " " + variableDeclarator.getName() + ";")
                    );
                });
            });
        }
    }
}
