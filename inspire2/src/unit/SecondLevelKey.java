package unit;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class SecondLevelKey {
	
	public String _hilbertCode;
	public PositionalQgram _qgram;
	
	public SecondLevelKey( String hilbertCode, PositionalQgram gram )
	{
		this._hilbertCode = hilbertCode;
		this._qgram = gram;
	}
	
	public SecondLevelKey( String hilberCodeQgram, int qgramLength )
	{
		String[] tempStr = hilberCodeQgram.split( ",", 2 );
		_hilbertCode = tempStr[0];		
		String positionalQgramStr = tempStr[1];
		_qgram = new PositionalQgram( 
				positionalQgramStr.substring(0, qgramLength),
				Integer.parseInt( positionalQgramStr.substring( qgramLength ) )
				);
	}

	
	@Override 
	public String toString()
	{
		return _hilbertCode + "," + _qgram;
	}
	
	@Override
	public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
           	append(_hilbertCode).
            append(_qgram).
            toHashCode();
    }

	@Override
    public boolean equals(Object obj) {
		
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (obj.getClass() != getClass())
            return false;

        SecondLevelKey key1 = (SecondLevelKey) obj;
        return new EqualsBuilder().
            append(this._hilbertCode, key1._hilbertCode).
            append(this._qgram, key1._qgram).
            isEquals();
    }
	
}
