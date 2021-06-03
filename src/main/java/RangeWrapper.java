import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

// TODo implement comparable
public class RangeWrapper implements Serializable{

    Class type;
    long base, step, size;
    int rank;
    String baseStr;
    TreeMap<String, Integer> strMap;
    TreeMap<Long, Long> intMap;
    private char [] l = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();

    public RangeWrapper(
        int rank,
        String columnName, 
        String columnType, 
        String columnMin, 
        String columnMax) throws ClassNotFoundException, DBAppException{

        this.base = this.step = this.size = 0L;
        this.rank = rank;
        strMap = new TreeMap<>();
        intMap = new TreeMap<>();
        this.type = Class.forName(columnType);
        if(!this._checkClass(type)){
            throw new DBAppException();
        }
        this._calculateRange(columnMin, columnMax);
    }

    // TODO check the  get parameter type
    public int getPos(Object value) throws DBAppException{
        
        long intVal = 0;
        if(this.type.equals(String.class)){
            return strMap.get(strMap.ceilingKey((String) value));
        }else if(type.equals(Date.class)){
            intVal = ((Date) value).getTime();
        }else if(type.equals(Integer.class)){
            intVal = (long) ((Integer) value);
        }else if(type.equals(Double.class)){
            intVal = (long) Math.ceil((Double)(value));
        }

        return (int) (intMap.get(intMap.ceilingKey(intVal)).intValue());
    }

    private void _handleSpecialString(String min, String max){
        Long range = 1L, minVal = Utils.convertUnknown(min);

        range = Utils.convertUnknown(max) - minVal + 1;
        
        this.size = Math.min(10, range);
        
        if(range <= 10)
            this.step = 1;
        else
            this.step = range / 10;
        
        long _c = 0;
        String base = Utils.getBase(min);
        
        for(long i=1; i<= this.size; i++){
            strMap.put(
                Utils.next(base, ((i != step) ? (step*i - 1) : (range - 1)) + minVal), 
                (int) _c++
            );
        }
    }

    private void _calculateRange(String min, String max) throws DBAppException{
        
        if(this.type.equals(String.class)){
            int minLen = min.length(), maxLen = max.length();
            long range = 0L;
            if(Pattern.matches(".*[^a-zA-Z].*", min)){
                this._handleSpecialString(min, max);
                return;
            }

            // calculate the variations of the strings
            for(int i= minLen; i<=maxLen; i++)
                range += (long) Math.pow(52, i);
            
            this.size = Math.min(10, range);
            this.baseStr = Utils.append(min, maxLen, '@');
            char [] _baseStr = this.baseStr.toCharArray(); 
            
            for(int i=0; i<_baseStr.length; i++)
                _baseStr[i] = (_baseStr[i] != '@') ?  'A' : '@';
            
            this.baseStr = new String(_baseStr);

            if(range <= 10 ){
                this.step = 1;
            }else{
                this.step = range / 10;
            }

            for(long i=1; i<=this.size; i++)
                strMap.put(
                    this._reach(this.baseStr, i != this.size ? (step * i - 1) : (range - 1)), 
                    (int) i-1
                );

            char [] _tmpMax = max.toCharArray();
            for(int i=0;i<_tmpMax.length; i++)
                _tmpMax[i] = 'z';
            strMap.put(new String(_tmpMax), (int) this.size - 1);

        }else if(type.equals(Date.class)){
            Date minDate = this._parseDate(min), maxDate = this._parseDate(max); 
            long range = maxDate.getTime() - minDate.getTime() + 1;

            this.size = Math.min(10, range);
            this.base = minDate.getTime();
            
            if(range <= 10){
                this.step = 1;
            }else{
                this.step = range / 10;
            }
            
            for(int i=1; i<=this.size; i++)
                intMap.put( 
                    ((i != this.size) ? (step * i - 1) : (range - 1)) + minDate.getTime(), 
                    (long) (i - 1)
                );

        }else if(type.equals(Integer.class)){
            Integer minInt = Integer.parseInt(min), maxInt = Integer.parseInt(max);
            long range = maxInt - minInt + 1;
            
            this.size = Math.min(10, range);
            this.base = minInt;
            
            if(range <= 10){
                this.step = 1;
            }else{
                this.step = range / 10;
            }

            for(int i=1; i<=this.size; i++)
                intMap.put(
                    ((i != this.size) ? (i * this.step - 1) : (range - 1)) + minInt,
                    (long) (i - 1)
                );
        }else if(type.equals(Double.class)){
            Double minDouble = Double.parseDouble(min), maxDouble = Double.parseDouble(max);
            long range = (long) Math.ceil(maxDouble - minDouble + 1);

            this.size = Math.min(10, range);
            this.base = (long) Math.floor(minDouble);
            
            if(range <= 10){
                this.step = 1;
            }else{
                this.step = range / 10;
            }
            for(int i=1; i<=this.size; i++)
                intMap.put(
                    (long) ((long) ((i != this.size) ? (this.step * i - 1) : (range - 1)) + Math.ceil(minDouble)),
                    (long) (i - 1)
                );
        }
    }

    private String _inc(String base) {
    	
    	StringBuilder _tmp =  new StringBuilder(base.substring(0,base.length()-1));
    	_tmp = _tmp.reverse();
    	char res[];
    	res=_tmp.toString().toCharArray();
		for (int i=0;i<res.length;i++) {
			if(res[i]!='z'){
				String _tmp2 = new String(l);
				res[i] = l[_tmp2.indexOf(res[i])+1];
				break;
			}
			else {
				res[i]='@';
			}
		}
    	return new StringBuilder(new String(res)).reverse().toString()+"@";
    }

    private String _reach(String base, long offset) {
    	while(offset > 0) {
    		if(base.contains("@")){
    			int _count = Utils.count(base, '@');
    			base = Utils.replaceM(base, (int) Math.min(_count, offset), '@', 'A');
    			offset -= Math.min(_count, offset);
    		}else{
    			if(offset >= 52) {
    				base = _inc(base);
    				offset -= 52;
    			}else{
    				char c = base.charAt(base.length()-1);
    				base = base.substring(0, base.length()-1);
    				String _tmp = new String(l);
    				base += l[_tmp.indexOf(c) + (int) offset];
    				offset=0;
    			}
    		}
    	}
    	return base;
    }

    private boolean _checkClass(Class type){
        boolean exists = false;
        for(Class classObj : new Class[]{String.class, Integer.class, Double.class, Date.class}){
            if(type.equals(classObj)){
                exists = true;
                break;
            }
        }
        return exists;
    }

    private Date _parseDate(String value) throws DBAppException {
        Date result = null;
        try {
            result = new SimpleDateFormat(Table.DateFormate).parse(value);
        } catch (ParseException e) {
            System.out.println("[ERROR] the input date is malformed");
            throw new DBAppException();
        }
        return result;
    }

}
