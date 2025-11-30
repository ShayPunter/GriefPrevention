/*
    GriefPrevention Server Plugin for Minecraft
    Copyright (C) 2012 Ryan Hamshire

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package me.ryanhamshire.GriefPrevention.storage;

import me.ryanhamshire.GriefPrevention.Claim;
import me.ryanhamshire.GriefPrevention.ClaimPermission;
import me.ryanhamshire.GriefPrevention.CustomLogEntryTypes;
import me.ryanhamshire.GriefPrevention.GriefPrevention;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Compact claim storage that consolidates all claims into world-based files
 * instead of individual files per claim. This dramatically reduces filesystem
 * overhead and improves performance on servers with many claims.
 *
 * File format: claims_{worldname}.dat (optionally gzipped)
 * Each claim is stored on a single line with pipe-separated fields.
 */
public class CompactClaimStore
{
    // Storage format version for future migrations
    private static final int FORMAT_VERSION = 1;

    // Field separator for claim data
    private static final char FIELD_SEP = '|';
    private static final char LIST_SEP = ';';
    private static final char PERM_SEP = ':';

    private final Path dataFolder;
    private final boolean useCompression;

    // Track dirty claims that need to be saved
    private final Set<Long> dirtyClaims = ConcurrentHashMap.newKeySet();
    private final ConcurrentLinkedQueue<Long> deletedClaims = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean saveInProgress = new AtomicBoolean(false);

    // Cache of all loaded claims by world
    private final Map<String, Map<Long, ClaimData>> claimsByWorld = new ConcurrentHashMap<>();

    // Next claim ID
    private long nextClaimId = 0;

    public CompactClaimStore(Path dataFolder, boolean useCompression)
    {
        this.dataFolder = dataFolder;
        this.useCompression = useCompression;
    }

    /**
     * Initialize the store and load all claims from disk.
     */
    public void initialize() throws IOException
    {
        if (!Files.exists(dataFolder))
        {
            Files.createDirectories(dataFolder);
        }

        // Load next claim ID
        loadNextClaimId();

        // Load claims for each world
        for (World world : Bukkit.getWorlds())
        {
            loadWorldClaims(world.getName());
        }
    }

    /**
     * Load all claims for a specific world.
     */
    public void loadWorldClaims(String worldName) throws IOException
    {
        Path claimFile = getClaimFilePath(worldName);

        if (!Files.exists(claimFile))
        {
            claimsByWorld.put(worldName, new ConcurrentHashMap<>());
            return;
        }

        Map<Long, ClaimData> worldClaims = new ConcurrentHashMap<>();

        try (BufferedReader reader = createReader(claimFile))
        {
            String line;
            int lineNum = 0;

            // First line is format version
            String versionLine = reader.readLine();
            lineNum++;
            if (versionLine == null || !versionLine.startsWith("V:"))
            {
                throw new IOException("Invalid claim file format: missing version header");
            }
            int version = Integer.parseInt(versionLine.substring(2));

            if (version > FORMAT_VERSION)
            {
                throw new IOException("Claim file version " + version + " is newer than supported version " + FORMAT_VERSION);
            }

            while ((line = reader.readLine()) != null)
            {
                lineNum++;
                if (line.isEmpty() || line.startsWith("#"))
                {
                    continue; // Skip empty lines and comments
                }

                try
                {
                    ClaimData claim = parseClaim(line, worldName);
                    if (claim != null)
                    {
                        worldClaims.put(claim.id, claim);

                        // Update next claim ID if necessary
                        if (claim.id >= nextClaimId)
                        {
                            nextClaimId = claim.id + 1;
                        }
                    }
                }
                catch (Exception e)
                {
                    GriefPrevention.AddLogEntry("Error parsing claim at line " + lineNum + " in " + claimFile + ": " + e.getMessage(), CustomLogEntryTypes.Exception);
                }
            }
        }

        claimsByWorld.put(worldName, worldClaims);
        GriefPrevention.AddLogEntry("Loaded " + worldClaims.size() + " claims for world: " + worldName);
    }

    /**
     * Parse a single claim from a line of text.
     * Format: id|ownerUUID|x1,y1,z1,x2,y2,z2|parentId|inheritNothing|B:uuid,uuid;C:uuid;A:uuid;M:uuid
     */
    private ClaimData parseClaim(String line, String worldName) throws Exception
    {
        String[] fields = splitFields(line);

        if (fields.length < 5)
        {
            throw new IllegalArgumentException("Not enough fields: " + fields.length);
        }

        ClaimData claim = new ClaimData();

        // Field 0: Claim ID
        claim.id = Long.parseLong(fields[0]);

        // Field 1: Owner UUID (empty for admin claims)
        if (!fields[1].isEmpty())
        {
            claim.ownerUUID = parseCompactUUID(fields[1]);
        }

        // Field 2: Coordinates (x1,y1,z1,x2,y2,z2)
        String[] coords = fields[2].split(",");
        if (coords.length != 6)
        {
            throw new IllegalArgumentException("Invalid coordinates");
        }
        claim.x1 = Integer.parseInt(coords[0]);
        claim.y1 = Integer.parseInt(coords[1]);
        claim.z1 = Integer.parseInt(coords[2]);
        claim.x2 = Integer.parseInt(coords[3]);
        claim.y2 = Integer.parseInt(coords[4]);
        claim.z2 = Integer.parseInt(coords[5]);
        claim.worldName = worldName;

        // Field 3: Parent claim ID (-1 for top-level)
        claim.parentId = Long.parseLong(fields[3]);

        // Field 4: inheritNothing flag
        claim.inheritNothing = "1".equals(fields[4]) || "true".equalsIgnoreCase(fields[4]);

        // Field 5: Permissions (optional)
        if (fields.length > 5 && !fields[5].isEmpty())
        {
            parsePermissions(fields[5], claim);
        }

        return claim;
    }

    /**
     * Parse permission string into claim data.
     * Format: B:uuid,uuid;C:uuid;A:uuid;M:uuid
     */
    private void parsePermissions(String permStr, ClaimData claim)
    {
        String[] permGroups = permStr.split(String.valueOf(LIST_SEP));

        for (String group : permGroups)
        {
            if (group.length() < 2 || group.charAt(1) != PERM_SEP)
            {
                continue;
            }

            char type = group.charAt(0);
            String[] uuids = group.substring(2).split(",");

            for (String uuidStr : uuids)
            {
                if (uuidStr.isEmpty()) continue;

                String id = uuidStr;
                // Check if it's a UUID or a permission node [node]
                if (!uuidStr.startsWith("["))
                {
                    try
                    {
                        // Try to parse as UUID
                        id = parseCompactUUID(uuidStr).toString();
                    }
                    catch (Exception e)
                    {
                        // Keep as-is if not a valid UUID (might be a permission node)
                        id = uuidStr;
                    }
                }

                switch (type)
                {
                    case 'B': // Build
                        claim.builders.add(id);
                        break;
                    case 'C': // Container
                        claim.containers.add(id);
                        break;
                    case 'A': // Access
                        claim.accessors.add(id);
                        break;
                    case 'M': // Manager
                        claim.managers.add(id);
                        break;
                }
            }
        }
    }

    /**
     * Convert a Claim object to ClaimData for storage.
     */
    public ClaimData claimToData(Claim claim)
    {
        ClaimData data = new ClaimData();
        data.id = claim.getID();
        data.ownerUUID = claim.ownerID;
        data.worldName = claim.getLesserBoundaryCorner().getWorld().getName();
        data.x1 = claim.getLesserBoundaryCorner().getBlockX();
        data.y1 = claim.getLesserBoundaryCorner().getBlockY();
        data.z1 = claim.getLesserBoundaryCorner().getBlockZ();
        data.x2 = claim.getGreaterBoundaryCorner().getBlockX();
        data.y2 = claim.getGreaterBoundaryCorner().getBlockY();
        data.z2 = claim.getGreaterBoundaryCorner().getBlockZ();
        data.parentId = claim.parent != null ? claim.parent.getID() : -1;
        data.inheritNothing = claim.getSubclaimRestrictions();

        // Get permissions
        ArrayList<String> builders = new ArrayList<>();
        ArrayList<String> containers = new ArrayList<>();
        ArrayList<String> accessors = new ArrayList<>();
        ArrayList<String> managers = new ArrayList<>();
        claim.getPermissions(builders, containers, accessors, managers);

        data.builders.addAll(builders);
        data.containers.addAll(containers);
        data.accessors.addAll(accessors);
        data.managers.addAll(managers);

        return data;
    }

    /**
     * Convert ClaimData to a Claim object.
     */
    public Claim dataToClaim(ClaimData data, Map<Long, Claim> claimIdMap)
    {
        World world = Bukkit.getWorld(data.worldName);
        if (world == null)
        {
            GriefPrevention.AddLogEntry("World not found for claim " + data.id + ": " + data.worldName, CustomLogEntryTypes.Debug);
            return null;
        }

        Location lesser = new Location(world, data.x1, data.y1, data.z1);
        Location greater = new Location(world, data.x2, data.y2, data.z2);

        Claim claim = new Claim(
            lesser,
            greater,
            data.ownerUUID,
            data.builders,
            data.containers,
            data.accessors,
            data.managers,
            data.inheritNothing,
            data.id
        );

        // Link parent if this is a subclaim
        if (data.parentId >= 0 && claimIdMap != null)
        {
            Claim parent = claimIdMap.get(data.parentId);
            if (parent != null)
            {
                claim.parent = parent;
            }
        }

        return claim;
    }

    /**
     * Format a claim for storage.
     * Format: id|ownerUUID|x1,y1,z1,x2,y2,z2|parentId|inheritNothing|permissions
     */
    public String formatClaim(ClaimData claim)
    {
        StringBuilder sb = new StringBuilder();

        // Field 0: ID
        sb.append(claim.id).append(FIELD_SEP);

        // Field 1: Owner UUID (compact format, no dashes)
        if (claim.ownerUUID != null)
        {
            sb.append(formatCompactUUID(claim.ownerUUID));
        }
        sb.append(FIELD_SEP);

        // Field 2: Coordinates
        sb.append(claim.x1).append(',')
          .append(claim.y1).append(',')
          .append(claim.z1).append(',')
          .append(claim.x2).append(',')
          .append(claim.y2).append(',')
          .append(claim.z2).append(FIELD_SEP);

        // Field 3: Parent ID
        sb.append(claim.parentId).append(FIELD_SEP);

        // Field 4: inheritNothing
        sb.append(claim.inheritNothing ? "1" : "0").append(FIELD_SEP);

        // Field 5: Permissions
        sb.append(formatPermissions(claim));

        return sb.toString();
    }

    /**
     * Format permissions for storage.
     */
    private String formatPermissions(ClaimData claim)
    {
        StringBuilder sb = new StringBuilder();

        appendPermissionList(sb, 'B', claim.builders);
        appendPermissionList(sb, 'C', claim.containers);
        appendPermissionList(sb, 'A', claim.accessors);
        appendPermissionList(sb, 'M', claim.managers);

        // Remove trailing semicolon
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == LIST_SEP)
        {
            sb.setLength(sb.length() - 1);
        }

        return sb.toString();
    }

    private void appendPermissionList(StringBuilder sb, char type, List<String> ids)
    {
        if (ids.isEmpty()) return;

        sb.append(type).append(PERM_SEP);
        for (int i = 0; i < ids.size(); i++)
        {
            if (i > 0) sb.append(',');
            String id = ids.get(i);
            // Compact UUID if it's a valid UUID
            if (!id.startsWith("["))
            {
                try
                {
                    UUID uuid = UUID.fromString(id);
                    id = formatCompactUUID(uuid);
                }
                catch (Exception e)
                {
                    // Keep as-is
                }
            }
            sb.append(id);
        }
        sb.append(LIST_SEP);
    }

    /**
     * Mark a claim as dirty (needs saving).
     */
    public void markDirty(Claim claim)
    {
        if (claim.getID() != null)
        {
            dirtyClaims.add(claim.getID());

            // Update in-memory cache
            String worldName = claim.getLesserBoundaryCorner().getWorld().getName();
            Map<Long, ClaimData> worldClaims = claimsByWorld.computeIfAbsent(worldName, k -> new ConcurrentHashMap<>());
            worldClaims.put(claim.getID(), claimToData(claim));
        }
    }

    /**
     * Mark a claim for deletion.
     */
    public void markDeleted(long claimId, String worldName)
    {
        deletedClaims.add(claimId);
        dirtyClaims.remove(claimId);

        Map<Long, ClaimData> worldClaims = claimsByWorld.get(worldName);
        if (worldClaims != null)
        {
            worldClaims.remove(claimId);
        }
    }

    /**
     * Check if there are pending changes to save.
     */
    public boolean hasPendingChanges()
    {
        return !dirtyClaims.isEmpty() || !deletedClaims.isEmpty();
    }

    /**
     * Save all dirty claims to disk.
     * This is designed to be called periodically and on shutdown.
     */
    public synchronized void saveAllDirty()
    {
        if (!hasPendingChanges())
        {
            return;
        }

        if (!saveInProgress.compareAndSet(false, true))
        {
            return; // Another save is already in progress
        }

        try
        {
            // Clear deleted claims from pending dirty list
            deletedClaims.clear();
            Set<Long> toSave = new HashSet<>(dirtyClaims);
            dirtyClaims.clear();

            // Group dirty claims by world
            Map<String, Set<Long>> dirtyByWorld = new HashMap<>();
            for (Long claimId : toSave)
            {
                for (Map.Entry<String, Map<Long, ClaimData>> entry : claimsByWorld.entrySet())
                {
                    if (entry.getValue().containsKey(claimId))
                    {
                        dirtyByWorld.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).add(claimId);
                        break;
                    }
                }
            }

            // Save each affected world file
            for (String worldName : dirtyByWorld.keySet())
            {
                saveWorldClaims(worldName);
            }

            // Save next claim ID
            saveNextClaimId();
        }
        catch (Exception e)
        {
            GriefPrevention.AddLogEntry("Error saving claims: " + e.getMessage(), CustomLogEntryTypes.Exception);
            e.printStackTrace();
        }
        finally
        {
            saveInProgress.set(false);
        }
    }

    /**
     * Force save all claims for all worlds.
     */
    public synchronized void saveAll()
    {
        try
        {
            for (String worldName : claimsByWorld.keySet())
            {
                saveWorldClaims(worldName);
            }
            saveNextClaimId();
            dirtyClaims.clear();
            deletedClaims.clear();
        }
        catch (Exception e)
        {
            GriefPrevention.AddLogEntry("Error saving all claims: " + e.getMessage(), CustomLogEntryTypes.Exception);
            e.printStackTrace();
        }
    }

    /**
     * Save all claims for a specific world.
     */
    private void saveWorldClaims(String worldName) throws IOException
    {
        Map<Long, ClaimData> worldClaims = claimsByWorld.get(worldName);
        if (worldClaims == null)
        {
            return;
        }

        Path claimFile = getClaimFilePath(worldName);
        Path tempFile = claimFile.resolveSibling(claimFile.getFileName() + ".tmp");

        try (BufferedWriter writer = createWriter(tempFile))
        {
            // Write version header
            writer.write("V:" + FORMAT_VERSION);
            writer.newLine();

            // Sort claims by ID for consistent output
            List<ClaimData> sortedClaims = new ArrayList<>(worldClaims.values());
            sortedClaims.sort(Comparator.comparingLong(c -> c.id));

            // Write parent claims first, then subclaims
            for (ClaimData claim : sortedClaims)
            {
                if (claim.parentId < 0)
                {
                    writer.write(formatClaim(claim));
                    writer.newLine();
                }
            }
            for (ClaimData claim : sortedClaims)
            {
                if (claim.parentId >= 0)
                {
                    writer.write(formatClaim(claim));
                    writer.newLine();
                }
            }
        }

        // Atomic rename
        Files.move(tempFile, claimFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Get the next available claim ID and increment the counter.
     */
    public synchronized long getNextClaimId()
    {
        return nextClaimId++;
    }

    /**
     * Set the next claim ID (used during migration).
     */
    public synchronized void setNextClaimId(long id)
    {
        if (id > nextClaimId)
        {
            nextClaimId = id;
        }
    }

    private void loadNextClaimId() throws IOException
    {
        Path idFile = dataFolder.resolve("_nextClaimID");
        if (Files.exists(idFile))
        {
            String content = Files.readString(idFile, StandardCharsets.UTF_8).trim();
            try
            {
                nextClaimId = Long.parseLong(content);
            }
            catch (NumberFormatException e)
            {
                GriefPrevention.AddLogEntry("Invalid next claim ID file, starting from 0", CustomLogEntryTypes.Debug);
                nextClaimId = 0;
            }
        }
    }

    private void saveNextClaimId() throws IOException
    {
        Path idFile = dataFolder.resolve("_nextClaimID");
        Files.writeString(idFile, String.valueOf(nextClaimId), StandardCharsets.UTF_8);
    }

    private Path getClaimFilePath(String worldName)
    {
        String extension = useCompression ? ".dat.gz" : ".dat";
        return dataFolder.resolve("claims_" + sanitizeWorldName(worldName) + extension);
    }

    private String sanitizeWorldName(String worldName)
    {
        // Replace characters that might cause filesystem issues
        return worldName.replaceAll("[^a-zA-Z0-9_-]", "_");
    }

    private BufferedReader createReader(Path file) throws IOException
    {
        InputStream is = Files.newInputStream(file);
        if (useCompression || file.toString().endsWith(".gz"))
        {
            is = new GZIPInputStream(is);
        }
        return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    }

    private BufferedWriter createWriter(Path file) throws IOException
    {
        OutputStream os = Files.newOutputStream(file);
        if (useCompression)
        {
            os = new GZIPOutputStream(os);
        }
        return new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
    }

    /**
     * Split a line by the field separator, handling edge cases.
     */
    private String[] splitFields(String line)
    {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();

        for (char c : line.toCharArray())
        {
            if (c == FIELD_SEP)
            {
                fields.add(current.toString());
                current.setLength(0);
            }
            else
            {
                current.append(c);
            }
        }
        fields.add(current.toString());

        return fields.toArray(new String[0]);
    }

    /**
     * Format UUID in compact form (no dashes).
     */
    private String formatCompactUUID(UUID uuid)
    {
        return uuid.toString().replace("-", "");
    }

    /**
     * Parse a compact UUID (with or without dashes).
     */
    private UUID parseCompactUUID(String str)
    {
        if (str.contains("-"))
        {
            return UUID.fromString(str);
        }

        // Insert dashes: 8-4-4-4-12
        if (str.length() != 32)
        {
            throw new IllegalArgumentException("Invalid compact UUID: " + str);
        }

        String formatted = str.substring(0, 8) + "-" +
                          str.substring(8, 12) + "-" +
                          str.substring(12, 16) + "-" +
                          str.substring(16, 20) + "-" +
                          str.substring(20);
        return UUID.fromString(formatted);
    }

    /**
     * Get all claims for a world.
     */
    public Map<Long, ClaimData> getWorldClaims(String worldName)
    {
        return claimsByWorld.getOrDefault(worldName, Collections.emptyMap());
    }

    /**
     * Get all claims across all worlds.
     */
    public Collection<ClaimData> getAllClaims()
    {
        List<ClaimData> all = new ArrayList<>();
        for (Map<Long, ClaimData> worldClaims : claimsByWorld.values())
        {
            all.addAll(worldClaims.values());
        }
        return all;
    }

    /**
     * Data class representing a stored claim.
     */
    public static class ClaimData
    {
        public long id;
        public UUID ownerUUID;
        public String worldName;
        public int x1, y1, z1, x2, y2, z2;
        public long parentId = -1;
        public boolean inheritNothing = false;
        public List<String> builders = new ArrayList<>();
        public List<String> containers = new ArrayList<>();
        public List<String> accessors = new ArrayList<>();
        public List<String> managers = new ArrayList<>();
    }
}
