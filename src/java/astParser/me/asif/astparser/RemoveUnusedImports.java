//package me.asif.astparser;
//
//import com.github.javaparser.JavaParser;
//import com.github.javaparser.ast.CompilationUnit;
//import com.github.javaparser.ast.ImportDeclaration;
//import com.github.javaparser.ast.Node;
//import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
//import com.github.javaparser.symbolsolver.javaparsermodel.JavaParserFacade;
//import com.github.javaparser.symbolsolver.javaparsermodel.declarations.JavaParserClassDeclaration;
//import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver;
//import com.github.javaparser.symbolsolver.resolution.typesolvers.ReflectionTypeSolver;
//import com.github.javaparser.symbolsolver.JavaSymbolSolver;
//import com.github.javaparser.ParserConfiguration;
//
//import java.io.File;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
//public class RemoveUnusedImports {
//
//    public static void main(String[] args) throws Exception {
//        // Create a CombinedTypeSolver and add the ReflectionTypeSolver
//        CombinedTypeSolver typeSolver = new CombinedTypeSolver();
//        typeSolver.add(new ReflectionTypeSolver());
//
//        // Configure the parser with the type solver
//        ParserConfiguration parserConfiguration = new ParserConfiguration()
//                .setSymbolResolver(new JavaSymbolSolver(typeSolver));
//
//        // Initialize JavaParser with the custom parser configuration
//        JavaParser javaParser = new JavaParser(parserConfiguration);
//
//        // Parse the Java source file
//        File sourceFile = new File("Path/To/YourJavaFile.java");
//        CompilationUnit compilationUnit = javaParser.parse(sourceFile).getResult().get();
//
//        // Collect all used types in the code
//        Set<String> usedTypes = new HashSet<>();
//        compilationUnit.accept(new VoidVisitorAdapter<Void>() {
//            public void visit(Node node, Void arg) {
//                if (node instanceof com.github.javaparser.ast.type.ClassOrInterfaceType) {
//                    usedTypes.add(((com.github.javaparser.ast.type.ClassOrInterfaceType) node).getNameAsString());
//                }
//                super.visit(node, arg);
//            }
//        }, null);
//
//        // Remove unused imports
//        List<ImportDeclaration> imports = compilationUnit.getImports();
//        imports.removeIf(importDeclaration -> {
//            String importName = importDeclaration.getNameAsString();
//            return usedTypes.stream().noneMatch(importName::endsWith);
//        });
//
//        // Print the updated code
//        System.out.println(compilationUnit.toString());
//    }
//}
//
//
