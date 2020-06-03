package de.adrianbartnik.job.parser;

import org.junit.Test;

import static de.adrianbartnik.job.parser.ParallelSocketArgumentParser.IsPowerMachineNumber;
import static de.adrianbartnik.job.parser.ParallelSocketArgumentParser.IsSingleDigit;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParallelSocketOperatorStateJobTest {

    @Test
    public void isPowerMachineNumber() {
        assertTrue(IsPowerMachineNumber("1"));
        assertTrue(IsPowerMachineNumber("2"));
        assertTrue(IsPowerMachineNumber("3"));
        assertTrue(IsPowerMachineNumber("4"));
        assertTrue(IsPowerMachineNumber("5"));
        assertTrue(IsPowerMachineNumber("6"));
        assertTrue(IsPowerMachineNumber("7"));
        assertTrue(IsPowerMachineNumber("8"));
        assertTrue(IsPowerMachineNumber("9"));
        assertFalse(IsPowerMachineNumber("0"));
        assertFalse(IsPowerMachineNumber("10"));
    }

    @Test
    public void isSingleDigit() {
        assertTrue(IsSingleDigit("0"));
        assertTrue(IsSingleDigit("1"));
        assertTrue(IsSingleDigit("2"));
        assertTrue(IsSingleDigit("3"));
        assertTrue(IsSingleDigit("4"));
        assertTrue(IsSingleDigit("5"));
        assertTrue(IsSingleDigit("6"));
        assertTrue(IsSingleDigit("7"));
        assertTrue(IsSingleDigit("8"));
        assertTrue(IsSingleDigit("9"));
        assertFalse(IsSingleDigit("10"));
    }
}