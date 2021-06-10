import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

public class DBAppParser {
    
    public static void main(String args[]){
        
        SQLiteLexer lexer = new SQLiteLexer(
            CharStreams.fromString("select s,n from x where s > 10 and n < '22'")
        );

        SQLiteParser parser = new SQLiteParser(
            new CommonTokenStream(lexer)
        );

        // System.out.println(parser.sql_stmt());
        parser.addParseListener(new SQLiteParserBaseListener(){
            @Override
            public void enterSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
                // System.out.println(ctx.select_core());
            }

            @Override
            public void exitSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
                // ctx.select_core;
                // for(SQLiteParser.Select_coreContext sc : ctx.select_core()){
                //     System.out.println(sc.result_column().get(0).expr().column_name().any_name().IDENTIFIER());
                //     System.out.println(sc.table_or_subquery().get(0).table_name().any_name().IDENTIFIER());
                //     for(SQLiteParser.ExprContext ec : sc.expr()){
                //         System.out.println(ec.expr().get(0).expr().get(1).literal_value().NUMERIC_LITERAL());
                //     }
                // }
            }

        });

        ParseTree pt = parser.parse();
        ParseTreeWalker walk = new ParseTreeWalker();
        walk.walk(
            new SQLiteParserBaseListener(){
                @Override
                public void enterSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
                    // System.out.println(ctx.select_core());
                }
    
                @Override
                public void exitSelect_stmt(SQLiteParser.Select_stmtContext ctx) {
                    // ctx.select_core;
                    System.out.print(ctx.getText());
                    for(SQLiteParser.Select_coreContext sc : ctx.select_core()){
                        System.out.println(sc.result_column().get(0).expr().column_name().any_name().IDENTIFIER());
                        System.out.println(sc.table_or_subquery().get(0).table_name().any_name().IDENTIFIER());
                        for(SQLiteParser.ExprContext ec : sc.expr()){
                            System.out.println(ec.expr().get(0).expr().get(1).literal_value().NUMERIC_LITERAL());
                        }
                    }
                }
    
            }, 
        pt);
    }
}
