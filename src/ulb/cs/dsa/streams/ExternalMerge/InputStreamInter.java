/**
 * 
 */
package ulb.cs.dsa.streams.ExternalMerge;


/**
 * @author JANANI CHAKKARADHARI,STEPHANY GARCIA
 *
 */
public interface InputStreamInter extends CommonInter{
	/*. method to get next integer*/
	int next();
	int getNumOfIntegers();
	void closeInputStream();
	long getPosition();
}
