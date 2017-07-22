

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class pig_Eval extends EvalFunc<Float> {

	String from, to;

	public pig_Eval(String from, String to) {
		super();
		this.from = from;
		this.to = to;
	}

    @Override
    public Float exec(Tuple input) throws IOException {
		// Make sure the input isn't null and is of the right size.
		if (input == null || input.size() != 1) return null;
		// do some magic lookup in a table
		// ...
		 float currency =Float.parseFloat( input.get(0).toString());
		 
		return currency;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.FLOAT));
    }

}
