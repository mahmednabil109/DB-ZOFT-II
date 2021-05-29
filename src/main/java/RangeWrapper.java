import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class RangeWrapper implements Serializable{

    Class type;
    long base, step, size;

    public RangeWrapper(
        String columnName, 
        String columnType, 
        String columnMin, 
        String columnMax) throws ClassNotFoundException, DBAppException{

        this.base = this.step = this.size = 0L;
        this.type = Class.forName(columnType);
        if(!this._checkClass(type)){
            throw new DBAppException();
        }
        this._calculateRange(columnMin, columnMax);
    }

    public int getPos(String value) throws DBAppException{
        
        if(this.type.equals(String.class)){
            //  handle string search
        }else if(type.equals(Date.class)){
            
        }else if(type.equals(Integer.class)){
            Integer intVal = Integer.parseInt(value);
            return (int) Math.min(this.size - 1, Math.ceil((intVal - this.base) / (this.step + 0.0)));
        }else if(type.equals(Double.class)){
            
        }
        return -1;
    }

    private void _calculateRange(String min, String max) throws DBAppException{
        
        if(this.type.equals(String.class)){
            int minLen = min.length(), maxLen = max.length();
            long range = 0L;
            // calculate the variations of the strings
            for(int i= minLen; i<=maxLen; i++)
                range += (long) Math.pow(26, i);
            
            this.size = Math.min(10, range);
            this.base = this._convert2_26(min);
            if(range <= 10 ){
                this.step = 1;
            }else{
                this.step = range / 10;
            }
        }else if(type.equals(Date.class)){
            Date minDate = this._parseDate(min), maxDate = this._parseDate(max); 
            long range = maxDate.getTime() - minDate.getTime() + 1;

            this.size = Math.min(10, range);
            this.base = minDate.getTime();
            
            if(this.size <= 10){
                this.step = 1;
            }else{
                this.step = range / 10;
            }
        }else if(type.equals(Integer.class)){
            Integer minInt = Integer.parseInt(min), maxInt = Integer.parseInt(max);
            long range = maxInt - minInt + 1;
            
            this.size = Math.min(10, range);
            this.base = minInt;
            
            if(this.size <= 10){
                this.step = 1;
            }else{
                this.step = range / 10;
            }
        }else if(type.equals(Double.class)){
            Double minDouble = Double.parseDouble(min), maxDouble = Double.parseDouble(max);
            long range = (long) Math.ceil(maxDouble - minDouble);

            this.size = Math.min(10, range);
            this.base = (long) Math.floor(minDouble);
            
            if(this.size <= 10){
                this.step = 1;
            }else{
                this.step = range / 10;
            }
        }
    }

    private long _convert2_26(String min){
        long ans = 0;
        char[] chrs = min.toCharArray();
        // convert to base26
        for(int i=0; i < chrs.length; i++) 
            ans += (chrs[chrs.length - i - 1] - 'a') * (long) Math.pow(26, i);
        System.out.println(ans);

        return ans;
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

    public static void main(String args[]){
        long ans = 0;
        char[] chrs = "id".toCharArray();
        // convert to base26
        for(int i=0; i < chrs.length; i++) 
            ans += (chrs[chrs.length - i - 1] - 'a') * (long) Math.pow(26, i);
        System.out.println(ans);
    }
}
