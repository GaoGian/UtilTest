

import java.io.Serializable;

public class AgentServerValue implements Serializable, Comparable<AgentServerValue> {
	private static final long serialVersionUID = -5369958269055656105L;
	private String hostName;
	private long initAgentTime; // 探针初始化时间
	private long latestActivity; // 最新活动时间
	private int MIN_DELAY_TIME = 1000 * 60 * 5; // 最少延迟间隔 5分钟

	private int instanceId;//实例ID
	private int serverId;
	private long ctime;
	private int applicationId;

	public AgentServerValue(){
		
	}
	public AgentServerValue(String hostName, long initAgentTime, long latestActivity ,int minDelayTime) {
		this.hostName = hostName;
		this.initAgentTime = initAgentTime;
		this.latestActivity = latestActivity;
		this.MIN_DELAY_TIME = minDelayTime;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public long getInitAgentTime() {
		return initAgentTime;
	}

	public void setInitAgentTime(long initAgentTime) {
		this.initAgentTime = initAgentTime;
	}

	public long getLatestActivity() {
		return latestActivity;
	}

	public void setLatestActivity(long latestActivity) {
		this.latestActivity = latestActivity;
	}

	public int getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(int instanceId) {
		this.instanceId = instanceId;
	}

	public int getServerId() {
		return serverId;
	}

	public void setServerId(int serverId) {
		this.serverId = serverId;
	}

	public long getCtime() {
		return ctime;
	}

	public void setCtime(long ctime) {
		this.ctime = ctime;
	}

	public int getApplicationId() {
		return applicationId;
	}

	public void setApplicationId(int applicationId) {
		this.applicationId = applicationId;
	}

	/**
	 * 按照最新活动时间 由大到小排序
	 */
	@Override
	public int compareTo(AgentServerValue o) {
		//如果时间间隔小于MIN_DELAY_TIME,按创建时间排序,反之按照最后访问时间排序
		if (this.latestActivity > o.latestActivity) {
			if ((this.latestActivity - o.latestActivity) > MIN_DELAY_TIME) { //A>b
				return -1;
			} else if ((this.latestActivity - o.latestActivity) < MIN_DELAY_TIME) {// A和B都活跃
				if (this.initAgentTime > o.initAgentTime) {//A的创建时间比B的晚.
					return 1;
				} else if (this.initAgentTime < o.initAgentTime) {
					return -1;
				} else {
					if(this.latestActivity> o.latestActivity){
						return -1;
					}else if(this.latestActivity < o.latestActivity){
						return 1;
					}else{
						return 0;
					}
				}
			}
		} else if (o.latestActivity > this.latestActivity) {
			if ((o.latestActivity - this.latestActivity) > MIN_DELAY_TIME) {
				return 1;
			} else if ((o.latestActivity - this.latestActivity) < MIN_DELAY_TIME) {
				if (o.initAgentTime > this.initAgentTime) {
					return -1;
				} else if (o.initAgentTime < this.initAgentTime) {
					return 1;
				} else {
					if(this.latestActivity > o.latestActivity){
						return -1;
					}else if(this.latestActivity < o.latestActivity){
						return 1;
					}else{
						return 0;
					}
				}
			}
		}

//		if (this.latestActivity > o.latestActivity) {
//			return -1;
//		}
//		if (this.latestActivity < o.latestActivity) {
//			return 1;
//		}
		
		return 0;
	}

	@Override
	public int hashCode() {
		int result = hostName != null ? hostName.hashCode() : 0;
		result = 31 * result + instanceId;
		result = 31 * result + serverId;
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AgentServerValue that = (AgentServerValue) o;

		if (instanceId != that.instanceId) return false;
		if (serverId != that.serverId) return false;
		return hostName != null ? hostName.equals(that.hostName) : that.hostName == null;

	}

}