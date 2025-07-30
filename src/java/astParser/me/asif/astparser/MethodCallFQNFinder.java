//package me.asif.astparser;
//
//import com.github.javaparser.JavaParser;
//import com.github.javaparser.ast.CompilationUnit;
//import com.github.javaparser.ast.body.MethodDeclaration;
//import com.github.javaparser.ast.expr.MethodCallExpr;
//import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
//import com.github.javaparser.resolution.MethodUsage;
//import com.github.javaparser.resolution.declarations.ResolvedMethodDeclaration;
//import com.github.javaparser.resolution.declarations.ResolvedReferenceTypeDeclaration;
//import com.github.javaparser.symbolsolver.JavaSymbolSolver;
//import com.github.javaparser.symbolsolver.model.resolution.TypeSolver;
//import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver;
//import com.github.javaparser.symbolsolver.resolution.typesolvers.JavaParserTypeSolver;
//
//import java.io.File;
//import java.util.HashSet;
//
//public class MethodCallFQNFinder {
//
//    public static void main(String[] args) throws Exception {
//        // Set up the type solver
//        TypeSolver typeSolver = new CombinedTypeSolver(new JavaParserTypeSolver(new File("path/to/your/source")));
//        JavaSymbolSolver symbolSolver = new JavaSymbolSolver(typeSolver);
//        JavaParser.getStaticConfiguration().setSymbolResolver(symbolSolver);
//
//        // Parse the source file
//        CompilationUnit cu = JavaParser.parse(new File("path/to/your/source/YourClass.java"));
//
//        // Visit methods and find method calls
//        cu.accept(new MethodVisitor(), null);
//    }
//
//    private static class MethodVisitor extends VoidVisitorAdapter<Void> {
//        @Override
//        public void visit(MethodDeclaration md, Void arg) {
//            super.visit(md, arg);
//
//            System.out.println("Method: " + md.getName());
//
//            // Visit and print all method calls inside this method
//            md.accept(new MethodCallVisitor(), null);
//        }
//    }
//
//    private static class MethodCallVisitor extends VoidVisitorAdapter<Void> {
//        HashSet<String> methodCallsFQN = new HashSet<>();
//        @Override
//        public void visit(MethodCallExpr methodCall, Void arg) {
//            super.visit(methodCall, arg);
//            try {
//                ResolvedMethodDeclaration resolvedMethod = methodCall.resolve();
////                get the name of the current method
//
//
//
////                System.out.println("Method Call: " + methodCall.getName());
//
//                // Get the class name where the method is declared
//                String className = resolvedMethod.getClassName();
////                System.out.println("Class Name: " + className);
//
//                // Get the FQN of the class
//                ResolvedReferenceTypeDeclaration declaringType = resolvedMethod.declaringType();
//                String classFQN = declaringType.getQualifiedName();
//                System.out.println("Fully Qualified Class Name: " + classFQN);
//
//                if(classFQN.startsWith("java"))
//                    return;
//
//                // Add the FQN of the method call to the set
//                methodCallsFQN.add(classFQN + ".java");
//
//
//
//            } catch (Exception e) {
//
//            }
//
//
//        }
//    }
//}
