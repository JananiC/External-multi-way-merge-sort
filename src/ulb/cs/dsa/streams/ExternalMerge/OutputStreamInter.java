package ulb.cs.dsa.streams.ExternalMerge;

/**
 * 
 * @author JANANI CHAKKARADHARI,STEPHANY GARCIA
 *
 */
public interface OutputStreamInter extends CommonInter{
void writeNext(int val);
void closeOutputStream();
}
