

public class WikiArticle {

	public int id;
	public String name;

	public WikiArticle(int id, String name) {
		this.id = id;
		this.name = name;
	}

	public String toString() {
		return "{\"id\":" + id + ",\"name\":\"" + name + "\"}";
	}
}