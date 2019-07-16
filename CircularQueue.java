import java.lang.reflect.Array;

public class CircularQueue<T> {

	private T[] q;
	private int size;
	private int s_ind;
	private int e_ind;

	public CircularQueue(Class<T> c, int initSize) {
		size = initSize;
		@SuppressWarnings("unchecked")
		final T[] a = (T[]) Array.newInstance(c, size);
		q = a;
		s_ind = 0;
		e_ind = 0;
	}

	public boolean isEmpty() {
		return s_ind == e_ind;
	}

	public void reset() {
		s_ind = 0;
		e_ind = 0;
	}

	public T poll() {
		if (e_ind == s_ind) {
			return null;
		}
		T ret = q[e_ind];
		e_ind = (e_ind + 1) % size;
		return ret;
	}

	public void add(T elem) {
		if ((s_ind+1) % size == e_ind) {
			resize();
		}
		q[s_ind] = elem;
		s_ind = (s_ind + 1) % size;
	}

	private void resize() {
		int newSize = size * 2;
		@SuppressWarnings("unchecked")
		T[] a = (T[]) Array.newInstance(
			q.getClass().getComponentType(), newSize);
		if (s_ind < e_ind) {
			System.arraycopy(q, e_ind, a, 0, size - e_ind);
			System.arraycopy(q, 0, a, size - e_ind, s_ind);
			s_ind = size + s_ind - e_ind;
			e_ind = 0;
		} else {
			System.arraycopy(q, e_ind, a, 0, s_ind - e_ind);
			s_ind = s_ind - e_ind;
			e_ind = 0;
		}
		size = newSize;
		q = a;
	}

	public int size() {
		int s = s_ind;
		if (s_ind < e_ind) {
			s += size;
		}
		return s - e_ind;
	}

}
