package unit;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class PositionalQgram implements Serializable, Comparable<PositionalQgram> {


	private static final long serialVersionUID = 1L;
	
	public String _qgram;
	public int _pos;
	
	
	public PositionalQgram(String qgram, int pos) 
	{
		_qgram = qgram;
		_pos = pos;
	}
	
	
	public PositionalQgram (PositionalQgram pgram) 
	{
		this._qgram = pgram._qgram;
		this._pos = pgram._pos;
	}
	
	public boolean hasSameToken(PositionalQgram gram2)
	{
		return _qgram.equals(gram2._qgram);
	}
	
	
	@Override 
	public String toString()
	{
		return _qgram + _pos;
	}
	
	@Override
	public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
           	append(_qgram).
            append(_pos).
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

        PositionalQgram qgram1 = (PositionalQgram) obj;
        return new EqualsBuilder().
            append(this._qgram, qgram1._qgram).
            append(this._pos, qgram1._pos).
            isEquals();
    }


	@Override
	public int compareTo(PositionalQgram posQgram1) 
	{
		if ( this == null && posQgram1 == null )
		{
			return 0;
		}
		else if ( this == null )
		{
			return -1;
		}
		else if ( posQgram1 == null )
		{
			return 1;
		}
		else 
		{
			int tokenComparedValue = this._qgram.compareTo(posQgram1._qgram);
			
			if ( tokenComparedValue != 0 )
			{
				return tokenComparedValue;
			} 
			else 
			{
				return Integer.compare( this._pos, posQgram1._pos );
			}	
		}
	}

	
}
