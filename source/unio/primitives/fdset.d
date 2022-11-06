module unio.primitives.fdset;

/** 
 * The specialized file descriptor set
 */
struct FDSet(Value)
{
    Value[int] data;
    alias data this;

    void remove(int key)
    {
        data.remove(key);
    }
}
