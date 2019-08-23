package org.corfudb.runtime.object;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang.ClassUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.ISerializer;

/**
 * Builds a wrapper for the underlying SMR Object.
 *
 * <p>Created by mwei on 11/11/16.
 */
public class CorfuCompileWrapperBuilder {
    private static int maxDepth(Class constructorType, Class argType, int currentDepth) {
        if (!ClassUtils.isAssignable(argType, constructorType, true)) {
            return currentDepth;
        } else {
            final int newDepth = currentDepth + 1;
            final List<Integer> results = new ArrayList<>();
            final Class[] interfaces = constructorType.getInterfaces();
            final Class superClass = constructorType.getSuperclass();

            Arrays.stream(interfaces)
                    .forEach(clazz -> results.add(maxDepth(interfaces[0], argType, newDepth)));
            Optional.ofNullable(superClass)
                    .map(clazz -> results.add(maxDepth(clazz, argType, newDepth)));
            return results.stream()
                    .max(Comparator.comparing(Integer::valueOf))
                    .orElse(newDepth);
        }
    }

    /**
     * Given the constructor arguments and the desired class type, find
     * the closest matching constructor. For example, if a wrapper
     * class contains two constructors, Constructor(A) and Constructor(B),
     * and the constructor argument is of type B, where B inherits from A,
     * instantiate the object via Constructor(B) since it is closes with
     * respect to type hierarchy.
     *
     * @param constructors   Object constructors.
     * @param args           Constructor arguments.
     * @param <T>            Type
     * @return instantiated  wrapper class.
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public static <T> Object findMatchingConstructor(
            Constructor[] constructors, Object[] args)
            throws IllegalAccessException, InvocationTargetException, InstantiationException {
        // Figure out the argument types.
        final List<Class> argTypes = Arrays
                .stream(args).map(Object::getClass)
                .collect(Collectors.toList());
        // Filter out the constructors that do not have the same arity.
        final List<Constructor> candidates = Arrays.stream(constructors)
                .filter(constructor -> constructor.getParameterTypes().length == args.length)
                .collect(Collectors.toList());

        Map<Integer, Constructor> matchingConstructors = new HashMap<>();
        for (Constructor constructor: candidates) {
            final List<Class> constructorTypes = Arrays.asList(constructor.getParameterTypes());
            List<Integer> resolutionList = new LinkedList<>();
            for (int idx = 0; idx < constructorTypes.size(); idx++) {
                int hierarchyDepth = 0; // 0 means unable to match the argument to the type.
                final Class<?> constructorType = constructorTypes.get(idx);
                Class<?> currentType = argTypes.get(idx);
                // If we are able to match the argument type with the constructor parameter
                // increment the hierarchy depth, signaling the distance between the
                // argument type and the parameter type in terms of type hierarchy.
                resolutionList.add(maxDepth(constructorType, currentType, 0));
            }

            // If any of the argument types has type distance of zero, this means
            // that we are unable to match the current constructor with the given arguments.
            if (resolutionList.stream().anyMatch(depth -> depth == 0)) {
                continue;
            }

            // Put all matching constructors in a map in form of:
            // (L1 Norm (Distance) -> Constructor)
            matchingConstructors.put(
                    resolutionList.stream().reduce(0, Integer::sum),
                    constructor);
        }

        // Instantiate the wrapper object with a constructor that has the lowest L1 norm.
        return matchingConstructors.entrySet().stream()
                .max(Map.Entry.comparingByKey()).orElseThrow(
                        () -> new IllegalStateException("No matching constructors found."))
                .getValue().newInstance(args);
    }

    /**
     * Returns a wrapper for the underlying SMR Object
     *
     * @param type       Type of SMR object.
     * @param rt         Connected instance of the CorfuRuntime.
     * @param streamID   StreamID of the SMR Object.
     * @param args       Arguments passed to instantiate the object.
     * @param serializer Serializer to be used to serialize the object arguments.
     * @param <T>        Type
     * @return Returns the wrapper to the object.
     * @throws ClassNotFoundException Class T not found.
     * @throws IllegalAccessException Illegal Access to the Object.
     * @throws InstantiationException Cannot instantiate the object using the arguments and class.
     */
    @SuppressWarnings("checkstyle:abbreviation")
    public static <T> T getWrapper(Class<T> type, CorfuRuntime rt,
                                   UUID streamID, Object[] args,
                                   ISerializer serializer)
            throws ClassNotFoundException, IllegalAccessException,
            InstantiationException, InvocationTargetException {
        // Do we have a compiled wrapper for this type?
        Class<ICorfuSMR<T>> wrapperClass = (Class<ICorfuSMR<T>>)
                Class.forName(type.getName() + ICorfuSMR.CORFUSMR_SUFFIX);

        // Instantiate a new instance of this class.
        ICorfuSMR<T> wrapperObject;
        if (args == null || args.length == 0) {
            wrapperObject = wrapperClass.newInstance();
        } else {
            wrapperObject =
                    (ICorfuSMR<T>) findMatchingConstructor(wrapperClass.getDeclaredConstructors(), args);
        }

        // Now we create the proxy, which actually manages
        // instances of this object. The wrapper delegates calls to the proxy.
        wrapperObject.setCorfuSMRProxy(new CorfuCompileProxy<>(rt, streamID,
                type, args, serializer,
                wrapperObject.getCorfuSMRUpcallMap(),
                wrapperObject.getCorfuUndoMap(),
                wrapperObject.getCorfuUndoRecordMap(),
                wrapperObject.getCorfuResetSet()));

        if (wrapperObject instanceof ICorfuSMRProxyWrapper) {
            ((ICorfuSMRProxyWrapper) wrapperObject)
                    .setProxy$CORFUSMR(wrapperObject.getCorfuSMRProxy());
        }

        return (T) wrapperObject;
    }
}
