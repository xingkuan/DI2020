package com.future.DI2020;

import java.util.function.BiFunction;
import java.util.function.Consumer;


interface LambdaFunction {
    void call();
}
interface Square {
    int calculate(int x); 
}

class LambdaTry {
    public static void main(String []args) {
        LambdaFunction lambdaFunction = () -> System.out.println("Hello world");
        lambdaFunction.call();
        
        Square s = (int x)->x*x;
        System.out.println(s.calculate(5));
        
        BiFunction<Integer, Integer, Integer> add = (a, b) -> a + b;
        System.out.println(add.apply(13, 2));
        
		//try fP
		//((DB2Data400) srcDB).tryFunctional("aa");
		//FunctionalTry s = (int x)->x*x ;
		DataPointer srcDB = DataPointer.dataPtrCreater("DB2D", "SRC");
		((DB2Data400)srcDB).tryFunctional("srcSch", "srcTbl", "journal" ,(int x)->x*x);
		

    }
}