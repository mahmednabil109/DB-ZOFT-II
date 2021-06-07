import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;
import java.util.stream.*;

public class Utils {
    
    // append the string `str` with `ch` to length `len`
    public static String append(String str, int len, char ch){
        StringBuilder sb = new StringBuilder(str);
        while(sb.length() < len)
            sb.append(ch);
        return sb.toString();
    }

    // replace at most `m` occurence of `ch` with `re` in string `str`
    public static String replaceM(String str, int m, char ch, char re){
        char arr[] = str.toCharArray();
        for(int i=0;i<arr.length && m > 0;i++)
            if(arr[i] == ch) {
                arr[i] = re; 
                m--;
            }
        return new String(arr);
    }

    // count the number of character `ch` in a string `str`
    public static int count(String str, char ch){
        int count = 0;
        char _tmp[] = str.toCharArray();
        for(int i=0;i<_tmp.length;i++)
            count += ch == _tmp[i] ? 1 : 0;
        return count;
    }

    public static Tuple wrapTuplePointer(TuplePointer tp){
        Hashtable<String, Object> data = new Hashtable<>();
        data.put(tp.PKName, tp.PKValue);

        // init dumb tuple to get it's position
        return new Tuple(tp.PKName, data);
    }

    // get the next string by moving the first character #offset steps
    public static String next(String str, long offset){
        // filter on the special characters
        char [] _tmps = str.replaceAll("[^a-zA-Z0-9]", "").toCharArray(); 
        // if the string is all special characters god onle knows what is the next one !
        if(_tmps.length == 0) return str;
        
        // handel the shift more efficient
        long [] weights = new long[_tmps.length];
        long [] factors = new long[_tmps.length];
        for(int i=0; i<_tmps.length; i++)
            if(_tmps[_tmps.length - i - 1] >= '0' && _tmps[_tmps.length - i - 1] <= '9')
                weights[i] = (i == 0) ?  10 : 10 * weights[i-1];
            else
                weights[i] = (i == 0) ?  26 : 26 * weights[i-1];

        final long[] _w = weights;
        weights = LongStream.range(0, weights.length).map(
            i -> _w[(int) (_w.length - i - 1)]
        ).toArray();

        for(int i=0; i < _tmps.length; i++){
            factors[i] = (i == _tmps.length - 1) ? offset : offset / weights[i+1]; 
            offset %= (i == _tmps.length - 1) ? 1 : weights[i+1];
        }

        for(int i=0; i<_tmps.length; i++)
            _tmps[i] += (int) factors[i];

        // perform the join with special characters
        char [] _str = str.toCharArray();
        StringBuilder res = new StringBuilder("");
        int i = 0,j = 0;
        while(i < _tmps.length && j < _str.length)
            if(!isSpecial(_str[j])){
                res.append(_tmps[i++]); 
                j ++ ;
            }else{
                res.append(_str[j++]);
            }
      
        return res.toString();
    }
    
    public static long convertUnknown (String str){
        
        str =  str.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
        char [] _tmps = str.toCharArray();
        long [] weights = new long[_tmps.length];
        for(int i=0; i<_tmps.length; i++)
            if(_tmps[_tmps.length - i - 1] >= '0' && _tmps[_tmps.length - i - 1] <= '9')
                weights[i] = (i == 0) ?  10 : 10 * weights[i-1];
            else
                weights[i] = (i == 0) ?  26 : 26 * weights[i-1];
        
        final long[] _w = weights;
        weights = LongStream.range(0, weights.length).map(
            i -> _w[(int) (_w.length - i - 1)]
        ).toArray();
        
        long res = 0L;
        for(int i=0; i<_tmps.length; i++){
            res += ((i == _tmps.length -1) ? 1 : weights[i + 1]) * (_tmps[i] - ((_tmps[i] >= '0' && _tmps[i] <= '9') ? '0' : 'a')); 
        }
        return res;
    }

    public static boolean isSpecial(char ch){
        return !(ch >= '0' && ch <= '9') && !(ch >= 'a' &&  ch <= 'z') && !(ch >= 'A' && ch <= 'Z');
    }

    public static String getBase(String str){
        char [] _tmps = str.toCharArray();
        for(int i=0;i<_tmps.length;i++)
            if(_tmps[i] >= '0' && _tmps[i] <= '9')
                _tmps[i] = '0';
            else if(_tmps[i] >= 'a' && _tmps[i] <= 'z')
                _tmps[i] = 'a';
            else if(_tmps[i] >= 'A' && _tmps[i] <= 'Z')
                _tmps[i] = 'A';
        return new String(_tmps);
    }

    // generate integers from s to e - 1
    public static Set<Integer> range(int s, int e){
        return IntStream.
                range(s, e).
                boxed().
                collect(Collectors.toSet());
    }

    public static boolean doesTupleMatch(Tuple t, Vector<SQLTerm> conditions) throws DBAppException{
        for(SQLTerm st : conditions){
            Comparable value = (Comparable) t.get(st._strColumnName);
            switch(st._strOperator){
                case "=":
                    if(value.compareTo(st._objValue) != 0) return false;
                    break;
                case ">=":
                    if(!(value.compareTo(st._objValue) >= 0)) return false;
                    break;
                case "<=":
                    if(!(value.compareTo(st._objValue) <= 0)) return false;
                    break;
                case ">":
                    if(!(value.compareTo(st._objValue) > 0)) return false;
                    break;
                case "<":
                    if(!(value.compareTo(st._objValue) < 0)) return false;
                    break;
                case "!=":
                    if(value.compareTo(st._objValue) == 0) return false;
                    break;
                default:
                    System.out.printf("[ERROR] operator %s is not defined \n", st._strOperator);
                    throw new DBAppException();
            }
        }
        return true;
    }

    public static void main(String args[]){
        // String [] arr = "asd1 asd2 asd3-asd4.asd5?asd6".split("[^a-zA-Z0-9]");
        // for(String str : arr)
        //     System.out.println(str);
        // String base = "00-0,a";
        // System.out.println(next("00-09-00"));
        // for(int i=0; i<26001; i++){
        //     System.out.println(base);
        //     base = next(base);
        // }
        // System.out.println("asd-asd-asd-as.dasd".replaceAll("[^a-zA-Z0-9]", ""));
        // String base = "00-0a";
        // for(int i=0; i<127;i++){
        //     System.out.println(base);
        //     base = next(base, 1);
        // }
        // System.out.println(next("00-aa", 25));
        // System.out.println(convertUnknown("00-1a"));
        System.out.println(getBase("43-0000"));
        // TODO remove
    }
}
