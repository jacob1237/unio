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

    /** 
     * Take value from the array and run a delegate on it if exists.
     * If the value is not present, execute the notFound() callback
     */
    void take(
        const Key key,
        void delegate (ref Value) @safe found,
        void delegate () @safe notFound
    ) {
        if (auto entry = key in data) {
            found(*entry);
        }
        else if (notFound !is null) {
            notFound();
        }
    }

    /** 
     * ditto, but without the notFound callback
     */
    void take(const Key key, void delegate (ref Value) @safe found)
    {
        take(key, found, null);
    }

    /** 
     * Returns a reference to the value, or inserts a new value and returns it
     */
    ref Value require(const Key key, lazy Value newVal = Value.init) return
    {
        return data.require(key, newVal);
    }

    void remove(const Key key)
    {
        data.remove(key);
    }
}
