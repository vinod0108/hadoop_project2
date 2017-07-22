import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;


public class Pig_Filter extends FilterFunc {

   


    public Boolean exec(Tuple input) throws IOException {
		 boolean isTargetAchieved =false;
        // expect  string
        String ValueToEval = input.get(0).toString();
    Float FloatValue = Float.parseFloat(ValueToEval);
    if ((FloatValue*100)>=80)
    {
    	return true;
    	
    }
    else
    {
    	return false;
    }
        
    }
}