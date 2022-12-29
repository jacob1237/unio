module unio.primitives.fdset;

@safe:

/** 
 * The specialized file descriptor set
 */
struct FDSet(Value)
{
    alias Key = int;

    Value[Key] data;
    alias data this;

    void get(Key key, void delegate (ref Value) @safe cb)
    {
        if (auto entry = key in data) {
            cb(*entry);
        }
    }

    void remove(int key)
    {
        data.remove(key);
    }
}
