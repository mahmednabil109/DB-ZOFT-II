import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Index implements Serializable {

    private Table context;
    private String[] columns;
    private Hashtable<String, RangeWrapper> dimentions;
    // linear Vector to simulate the
    Vector<IndexPage> data;

    public Index(Table context, String[] columnNames, Hashtable<String, String> columnsNameType,
            Hashtable<String, String> columnMin, Hashtable<String, String> columnMax)
            throws ClassNotFoundException, DBAppException {

        this.context = context;
        this.columns = Arrays.copyOf(columnNames, columnNames.length);
        this.dimentions = new Hashtable<>();
        this.data = new Vector<>((int) Math.pow(10, columnNames.length));
        int rank = 0;

        for (String columnName : columnNames)
            this.dimentions.put(
                columnName,
                new RangeWrapper(
                    rank++, 
                    columnName, 
                    columnsNameType.get(columnName),
                    columnMin.get(columnName), 
                    columnMax.get(columnName)
                )   
            );
        this._fill();
    }

    public void add(Tuple t, TuplePointer tp){

    }

    private void _fill() throws DBAppException {
        
        Hashtable<Integer,Vector<TuplePointer>> pagePack = new Hashtable<>();

        // generate the positions and group it by page to optimize the io operation
        for (int i = 0; i < this.context.buckets.size(); i++) {
            Page tPage = this.context.buckets.get(i);
            tPage.load();
            for (int j = 0; j < tPage.data.size(); j++) {
                // TODO add other methods in both tupe & tuplePointer to handle update and delete
                // TODO we could use SUB/PUB pattern
                Tuple tuple = tPage.data.get(j);
                TuplePointer tp = new TuplePointer(i, j);
                for(int pos : this._getPositions(this._formateArgs(tuple))){
                    Vector<TuplePointer> _tmp = (pagePack.containsKey(pos) ? pagePack.get(pos) : new Vector<>());
                    _tmp.add(tp);
                    pagePack.put(pos, _tmp);
                }
            }
            tPage.free();
        }
        // write the data to the pages and save them
        for(Map.Entry<Integer, Vector<TuplePointer>> entries : pagePack.entrySet()){
            // TODO
            IndexPage iPage = new IndexPage(this.context, "path");
            iPage.load();
            for(TuplePointer tp : entries.getValue())
                iPage.insert(tp);
            this.data.set(entries.getKey(), iPage);
            iPage.save();
        }
    }

    private Vector<SQLTerm> _formateArgs(Tuple tuple){
        Vector<SQLTerm> res = new Vector<>();
        for(String columnName : this.columns){
            SQLTerm sqlTerm = new SQLTerm();
            sqlTerm._strTableName = this.context.name;
            sqlTerm._strColumnName = columnName;
            sqlTerm._strOperator = "=";
            sqlTerm._objValue = tuple.get(columnName);
            res.add(sqlTerm);
        }
        return res;
    }

    // TODO test this method
    private int[] _getPositions(Vector<SQLTerm> columns) throws DBAppException {
        // vector that made to be send to _permute
        Vector<Vector<Integer>> sets = new Vector<>();
        // hashtable that collects the conditions with columns - incase there are multiple -.
        Hashtable<String, Vector<SQLTerm>> colPack = new Hashtable<>();

        for(SQLTerm st : columns){
            Vector<SQLTerm> _tmp;
            
            if(colPack.containsKey(st._strColumnName))
               _tmp = colPack.get(st._strColumnName);
            else
                _tmp = new Vector<>();

            _tmp.add(st);
            colPack.put(st._strColumnName, _tmp);
        }

        // merging the resutls to get the positions
        for(String column : this.columns){
            if(colPack.containsKey(column)){
                Vector<SQLTerm> conditions = colPack.get(column);
                RangeWrapper rw = this.dimentions.get(column);
                Set<Integer> _pos = Utils.range(0, (int) rw.size);
                for(SQLTerm st : conditions){
                    Set<Integer> _tmp = new HashSet<>();
                    int i = rw.getPos(st._objValue);
                    switch(st._strOperator){
                        case "=": _tmp.add(i); break;
                        case "!=": _tmp = Utils.range(0, (int) rw.size); break;
                        case ">":
                        case ">=": _tmp = Utils.range(i, (int) rw.size); break;
                        case "<":
                        case "<=": _tmp = Utils.range(0, i + 1); break;
                        default:
                            System.out.printf("[ERROR] this oprator %s is not defined", st._strOperator);
                            throw new DBAppException();
                    }
                    _pos.retainAll(_tmp);
                }
                // if there was a conditions that resulted in an empty positions then return {};
                if(_pos.size() == 0) return (new int[0]);
                // add the result to the sets
                sets.add(
                    _pos.stream().collect(Collectors.toCollection(Vector::new))
                );
            }else{
                // get the dimention size to evaluate partial query
                int dSize = (int) this.dimentions.get(column).size;
                sets.add(
                    IntStream.
                    range(0, dSize).
                    boxed().
                    collect(Collectors.toCollection(Vector::new))
                );
            }
        }

        // compute the product of the resulted sets to get the positions
        Vector<Vector<Integer>> pos = this._permute(sets);
        // compute the mapped positions in the array
        int[] res = IntStream.
                    range(0, pos.size()).
                    map(p -> this._calcPos(pos.get(p))).
                    toArray();

        return res;
    }

    public Vector<Tuple> search(Vector<SQLTerm> columns) throws DBAppException {
        Vector<Tuple> res = new Vector<>();
        //  here goes the dE7k
        // get the pages to filter the needed tuples
        Vector<IndexPage> pages = Arrays.stream(this._getPositions(columns)).
                                    boxed().
                                    map(i -> this.data.get(i)).
                                    collect(
                                        Collectors.toCollection(Vector::new)
                                    );
        
        // get the needed tuples after evaluating the tuple pointers
        for(IndexPage iPage : pages){
            iPage.load();
            res.addAll(iPage.get(columns));
            iPage.free();
        }

        return res;
    }



    private Vector<Vector<Integer>> _permute(Vector<Vector<Integer>> pos) {
        return null;
    }

    private int _calcPos(Vector<Integer> pos) {
        return 0;
    }

    public static void main(String args[]) {
        // int arr [] = {1, 2, 3, 4, 5};
        // System.out.println(
        //     Arrays.stream(arr).
        //     boxed().
        //     map(i -> i + 2).
        //     collect(
        //        Collectors.toList()
        //     )
        // );
        // System.out.println(IntStream.range(0, 10).boxed().collect(Collectors.toList()));
        // Set<Integer> set = new HashSet<Integer>();
        // for(int i : arr) set.add(i);
        // System.out.println(
        //     set.stream().
        //     collect(Collectors.toCollection(Vector::new))
        // );

        Set<Integer> s1 = new HashSet<>();
        Set<Integer> s2 = new HashSet<>();
        s1.add(1); s1.add(2); s1.add(3);
        s2.add(4);
        s1.retainAll(s2);
        System.out.println(s1);
        System.out.println(s2);

    }
}
