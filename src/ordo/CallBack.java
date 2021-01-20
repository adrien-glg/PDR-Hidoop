package ordo;

import java.rmi.*;

public interface CallBack extends Remote {
	
	public void incr() throws RemoteException ;
	
	public boolean isFinished() throws RemoteException;
}
