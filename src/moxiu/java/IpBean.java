package moxiu.java;

import java.io.Serializable;

/**
 * Created by w003 on 2016/4/22.
 */
public class IpBean implements Comparable<IpBean>, Serializable {

	private static final long serialVersionUID = 1245158545L;

	private String start;
	private String end;

	public IpBean(String start, String end) {
		this.start = start;
		this.end = end;
	}

	public void setEnd(String end) {
		this.end = end;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public String getStart() {
		return start;
	}


	public int compareTo(IpBean o) {
		int res = 0;
		if (o.getEnd().compareTo(this.start) < 0) {
			res = -1;
		}
		if (o.getStart().compareTo(this.start) > 0) {
			res = 1;
		}
		return res;
	}
}
