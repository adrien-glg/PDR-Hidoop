package ordo;

import java.rmi.*;

public interface CallBack extends Remote {
	
	public void incr();
	
	public boolean isFinished();
}
